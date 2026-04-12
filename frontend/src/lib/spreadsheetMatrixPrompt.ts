export const SPREADSHEET_MATRIX_RECOMMENDED_SYSTEM_PROMPT =
  "Use the spreadsheet decision matrix to select the single best matching row and column for the user's request. " +
  "Infer the strongest fit from the full context, reconcile ambiguous titles using actual responsibilities and scope, " +
  "and avoid shallow keyword matching.";

export const SPREADSHEET_MATRIX_RECOMMENDED_USER_MESSAGE_TEMPLATE = [
  "Analyze the context below and choose the matrix cell that best matches the strongest underlying fit.",
  "",
  "Pay close attention to what the subject is most likely to care about or respond to.",
  "If a role title is broad, ambiguous, or prestige-coded, use the detailed responsibilities, tools, and outcomes to interpret it more precisely.",
  "",
  "Context:",
  "{input_payload}",
].join("\n");

export const SPREADSHEET_MATRIX_RUNTIME_GUIDANCE = [
  "Built-in runtime guidance automatically added by this node:",
  "",
  "System guidance:",
  "- Use the full context to infer the strongest fit, not shallow keyword overlap or title matching.",
  "- Reason from responsibilities, incentives, technical depth, seniority, and likely response behavior.",
  "- If a headline title conflicts with the role description, prioritize the day-to-day work, tools, outcomes, and scope in the description.",
  "- Distinguish closely related profiles when the evidence supports it, such as technical versus managerial PMs.",
  "",
  "Selection guidance:",
  "- Match on the underlying role, intent, pressure, and likely response pattern, not just literal word overlap.",
  "- Infer what the person is most likely to respond to from their scope, current responsibilities, technical fluency, seniority, and business context.",
  "- Trust detailed responsibilities over a broad or generic title when they point in different directions.",
].join("\n");
