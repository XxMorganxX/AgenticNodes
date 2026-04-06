type ResponseSchemaPreset = {
  id: string;
  label: string;
  description: string;
  schemaText: string;
};

export const RESPONSE_SCHEMA_TEXT_CONFIG_KEY = "response_schema_text";

export const RESPONSE_SCHEMA_PRESETS: ResponseSchemaPreset[] = [
  {
    id: "string",
    label: "Plain Text",
    description: "Single freeform response string.",
    schemaText: JSON.stringify(
      {
        type: "string",
      },
      null,
      2,
    ),
  },
  {
    id: "answer",
    label: "Answer Object",
    description: "Answer plus an optional next step field.",
    schemaText: JSON.stringify(
      {
        type: "object",
        additionalProperties: false,
        properties: {
          answer: {
            type: "string",
            description: "Primary response shown to the user.",
          },
          next_step: {
            type: "string",
            description: "What the next node or consumer should do with this output.",
          },
        },
        required: ["answer"],
      },
      null,
      2,
    ),
  },
  {
    id: "answer_with_citations",
    label: "Answer + Citations",
    description: "Structured answer with supporting references.",
    schemaText: JSON.stringify(
      {
        type: "object",
        additionalProperties: false,
        properties: {
          answer: {
            type: "string",
            description: "Primary response shown to the user.",
          },
          citations: {
            type: "array",
            description: "Supporting references or evidence items.",
            items: {
              type: "object",
              additionalProperties: false,
              properties: {
                title: { type: "string" },
                source: { type: "string" },
                excerpt: { type: "string" },
              },
              required: ["title"],
            },
          },
        },
        required: ["answer"],
      },
      null,
      2,
    ),
  },
  {
    id: "status_data",
    label: "Status + Data",
    description: "Useful when downstream nodes branch on status fields.",
    schemaText: JSON.stringify(
      {
        type: "object",
        additionalProperties: false,
        properties: {
          status: {
            type: "string",
            enum: ["ok", "needs_input", "error"],
          },
          message: {
            type: "string",
          },
          data: {
            type: ["object", "array", "null"],
          },
        },
        required: ["status"],
      },
      null,
      2,
    ),
  },
  {
    id: "array",
    label: "Rows / Items",
    description: "Top-level array of labeled objects.",
    schemaText: JSON.stringify(
      {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            label: {
              type: "string",
            },
            value: {},
          },
          required: ["label", "value"],
        },
      },
      null,
      2,
    ),
  },
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function summarizeSchemaType(schema: Record<string, unknown>): string {
  const typeValue = schema.type;
  if (typeof typeValue === "string" && typeValue.trim().length > 0) {
    return typeValue;
  }
  if (Array.isArray(typeValue)) {
    const normalizedTypes = typeValue.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0);
    if (normalizedTypes.length > 0) {
      return normalizedTypes.join(" | ");
    }
  }
  if (isRecord(schema.properties)) {
    return "object";
  }
  if (isRecord(schema.items)) {
    return "array";
  }
  return "custom";
}

export function parseResponseSchemaText(value: string): {
  parsedSchema: Record<string, unknown> | null;
  schemaError: string | null;
} {
  if (value.trim().length === 0) {
    return {
      parsedSchema: null,
      schemaError: null,
    };
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    return {
      parsedSchema: isRecord(parsed) ? parsed : null,
      schemaError: isRecord(parsed) ? null : "Schema JSON must be an object.",
    };
  } catch (error) {
    return {
      parsedSchema: null,
      schemaError: error instanceof Error ? error.message : "Schema JSON is invalid.",
    };
  }
}

export function resolveResponseSchemaDetails(
  config: Record<string, unknown>,
): {
  schemaText: string;
  parsedSchema: Record<string, unknown> | null;
  schemaError: string | null;
  activeSchema: Record<string, unknown> | null;
  statusLabel: string;
} {
  const configuredText =
    typeof config[RESPONSE_SCHEMA_TEXT_CONFIG_KEY] === "string"
      ? String(config[RESPONSE_SCHEMA_TEXT_CONFIG_KEY])
      : null;
  const activeSchema = isRecord(config.response_schema) ? config.response_schema : null;
  const schemaText = configuredText ?? (activeSchema ? JSON.stringify(activeSchema, null, 2) : "");
  const { parsedSchema, schemaError } = parseResponseSchemaText(schemaText);
  const summarySource = activeSchema ?? parsedSchema;

  if (configuredText !== null && schemaError) {
    return {
      schemaText,
      parsedSchema,
      schemaError,
      activeSchema,
      statusLabel: "Draft has invalid JSON",
    };
  }

  if (summarySource) {
    return {
      schemaText,
      parsedSchema,
      schemaError,
      activeSchema,
      statusLabel: `Custom ${summarizeSchemaType(summarySource)} schema`,
    };
  }

  return {
    schemaText,
    parsedSchema,
    schemaError,
    activeSchema,
    statusLabel: "Default flexible payload",
  };
}
