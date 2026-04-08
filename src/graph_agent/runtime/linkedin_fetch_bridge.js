#!/usr/bin/env node

const fs = require("fs/promises");
const path = require("path");

function sleep(ms) {
  if (ms > 0) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
  return Promise.resolve();
}

function serializeError(error) {
  if (error instanceof Error) {
    return {
      type: "linkedin_fetch_bridge_error",
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  return {
    type: "linkedin_fetch_bridge_error",
    message: String(error),
  };
}

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function getStorageStatePath(linkedinDataDir, overridePath) {
  const candidate = String(overridePath || "").trim()
    ? path.resolve(String(overridePath).trim())
    : path.join(linkedinDataDir, ".auth", "linkedin-storage-state.json");
  try {
    await fs.access(candidate);
    return candidate;
  } catch (_) {
    return null;
  }
}

async function saveStorageState(context, targetPath) {
  await fs.mkdir(path.dirname(targetPath), { recursive: true });
  await context.storageState({ path: targetPath });
}

async function waitForManualLogin(page, context, storageStatePath, navigationTimeoutMs) {
  const startedAt = Date.now();
  process.stderr.write("LinkedIn authentication is required. Complete login in the opened browser window.\n");

  while (Date.now() - startedAt < navigationTimeoutMs) {
    const url = page.url();
    if (
      url.includes("/feed") ||
      url.includes("/mynetwork") ||
      url.includes("/jobs") ||
      url.includes("/messaging") ||
      url.includes("/in/") ||
      url.includes("/public-profile/")
    ) {
      if (storageStatePath) {
        await saveStorageState(context, storageStatePath);
      }
      return;
    }
    await page.waitForTimeout(1000);
  }

  throw new Error("Timed out waiting for LinkedIn manual login to complete.");
}

async function autoScrollProfile(page) {
  let previousSignature = "";
  let stablePasses = 0;

  for (let pass = 0; pass < 10; pass += 1) {
    const metrics = await page.evaluate(async () => {
      const isScrollable = (element) => {
        const style = window.getComputedStyle(element);
        const overflowY = style.overflowY;
        return (
          element.scrollHeight - element.clientHeight > 80 &&
          (overflowY === "auto" || overflowY === "scroll" || element === document.scrollingElement)
        );
      };

      const candidates = [document.scrollingElement, ...document.querySelectorAll("*")]
        .filter(Boolean)
        .filter((element) => isScrollable(element))
        .map((element) => ({
          element,
          clientHeight: element.clientHeight,
          scrollHeight: element.scrollHeight,
        }))
        .sort((a, b) => b.scrollHeight - a.scrollHeight)
        .slice(0, 5);

      const stepDelay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
      const touched = [];

      for (const candidate of candidates) {
        const el = candidate.element;
        const step = Math.max(500, Math.floor((el.clientHeight || window.innerHeight) * 0.9));
        let y = 0;

        while (y < el.scrollHeight) {
          el.scrollTo(0, y);
          y += step;
          await stepDelay(350);
        }

        el.scrollTo(0, el.scrollHeight);
        await stepDelay(1000);

        touched.push({
          scrollHeight: el.scrollHeight,
          finalScrollTop: el.scrollTop,
        });
      }

      const bodyText = document.body.innerText || "";
      const headings = ["About", "Experience", "Education", "Projects", "Skills"].filter((heading) =>
        bodyText.includes(heading),
      );

      return {
        documentHeight: Math.max(document.body.scrollHeight, document.documentElement.scrollHeight),
        touched,
        headings,
      };
    });

    const signature = JSON.stringify({
      documentHeight: metrics.documentHeight,
      touched: metrics.touched,
      headings: metrics.headings,
    });

    if (signature === previousSignature) {
      stablePasses += 1;
    } else {
      stablePasses = 0;
    }
    previousSignature = signature;

    if (
      stablePasses >= 1 &&
      (metrics.headings.includes("Experience") ||
        metrics.headings.includes("Education") ||
        metrics.headings.includes("Projects"))
    ) {
      break;
    }
  }

  await page.evaluate(() => {
    window.scrollTo(0, 0);
    for (const element of document.querySelectorAll("*")) {
      if (element.scrollTop) {
        element.scrollTo(0, 0);
      }
    }
  });
}

async function gotoProfile(page, url, navigationTimeoutMs, pageSettleMs) {
  await page.goto(url, {
    waitUntil: "domcontentloaded",
    timeout: navigationTimeoutMs,
  });

  try {
    await page.waitForLoadState("load", {
      timeout: Math.min(navigationTimeoutMs, 15000),
    });
  } catch (_) {
    // LinkedIn often keeps background activity alive; best-effort load is enough.
  }

  await sleep(pageSettleMs);
}

async function fetchProfile(payload) {
  const linkedinDataDir = path.resolve(String(payload.linkedinDataDir || "").trim());
  const extractorPath = path.join(linkedinDataDir, "linkedin-html-extractor.js");
  const playwrightPath = path.join(linkedinDataDir, "node_modules", "playwright");
  const { extractLinkedInProfileFromHtml } = require(extractorPath);
  const { chromium } = require(playwrightPath);

  const storageStatePath = await getStorageStatePath(linkedinDataDir, payload.sessionStatePath);
  const headless = Boolean(payload.headless);
  const navigationTimeoutMs = Number(payload.navigationTimeoutMs || 45000);
  const pageSettleMs = Number(payload.pageSettleMs || 3000);

  const browser = await chromium.launch({
    headless,
    slowMo: headless ? 0 : 50,
  });

  try {
    const context = await browser.newContext(storageStatePath ? { storageState: storageStatePath } : {});
    const page = await context.newPage();
    page.setDefaultNavigationTimeout(navigationTimeoutMs);
    page.setDefaultTimeout(navigationTimeoutMs);

    await gotoProfile(page, payload.url, navigationTimeoutMs, pageSettleMs);
    await autoScrollProfile(page);
    let html = await page.content();
    let extracted = extractLinkedInProfileFromHtml(html);

    if (extracted.pageType === "auth_wall" && !headless) {
      await page.goto("https://www.linkedin.com/login", {
        waitUntil: "domcontentloaded",
        timeout: navigationTimeoutMs,
      });
      await waitForManualLogin(
        page,
        context,
        storageStatePath || path.join(linkedinDataDir, ".auth", "linkedin-storage-state.json"),
        navigationTimeoutMs,
      );
      await gotoProfile(page, payload.url, navigationTimeoutMs, pageSettleMs);
      await autoScrollProfile(page);
      html = await page.content();
      extracted = extractLinkedInProfileFromHtml(html);
    }

    return {
      extracted,
      finalPageUrl: page.url(),
      storageStatePath: storageStatePath || path.join(linkedinDataDir, ".auth", "linkedin-storage-state.json"),
    };
  } finally {
    await browser.close();
  }
}

async function main() {
  const raw = await readStdin();
  const payload = JSON.parse(raw || "{}");
  const result = await fetchProfile(payload);
  process.stdout.write(JSON.stringify(result));
}

main().catch((error) => {
  process.stdout.write(JSON.stringify({ error: serializeError(error) }));
  process.exit(1);
});
