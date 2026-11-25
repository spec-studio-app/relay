// The one place for the tool names, descriptions and input schemas.
// The agent on the Mac (`studio agent`) implements the same names and applies
// the defaults. The relay passes the arguments to the agent as given.
//
// Write each description in ASD-STE100 Simplified Technical English: short
// sentences (25 words or fewer), active voice, and the Spec Studio terms spec,
// stage, status and turn. A test checks this.
import { z } from "zod";

export interface ToolDefinition {
  name: string;
  title: string;
  description: string;
  inputSchema: Record<string, z.ZodType>;
}

const repo = z
  .string()
  .min(1)
  .describe("The path of the repo on the Mac. Use a path that list_repos gives.");

const slug = z.string().min(1).describe("The slug of the spec. Use a slug that list_specs gives.");

export const TOOLS: ToolDefinition[] = [
  {
    name: "list_repos",
    title: "List the repos",
    description:
      "List the repos in Spec Studio, in sidebar order. Each repo shows its path on the Mac. Use the path as the repo argument of the other tools. " +
      "When the Mac is offline, the list comes from the last connection of the Mac, and the result gives its time.",
    inputSchema: {},
  },
  {
    name: "list_specs",
    title: "List the specs",
    description:
      "List the open specs of Spec Studio. Each spec shows its title, slug, mode, stage, status, note and turn. " +
      "The list also shows whether the spec needs the user. " +
      "When Spec Studio does not run, the turn comes from state.json, and the result says so. " +
      "When the Mac is offline, the list comes from the relay. Each spec then shows the time of its last sync.",
    inputSchema: {
      repo: repo.optional().describe("Show only the specs of this repo. Give the path of the repo on the Mac."),
      all: z.boolean().optional().describe("Also show the specs that are merged or closed."),
    },
  },
  {
    name: "read_spec",
    title: "Read a spec",
    description:
      "Read one spec. The result shows the state of the spec: its stage, status, note, mode and PRs. " +
      "It also shows the turn of the spec and whether the spec needs the user. " +
      "It also shows spec.md, plan.md when it exists, and the last 20 events of the activity. " +
      "When the Mac is offline, the result comes from the relay and shows the time of the last sync. " +
      "Then spec.md and plan.md are not available.",
    inputSchema: { repo, slug },
  },
  {
    name: "make_spec",
    title: "Make a spec",
    description:
      "Make a new spec in a repo with a title and a brief. The brief goes into spec.md. " +
      "The mode is autopilot or guided. Remote Control is on when you do not give a value. " +
      "Set start to true to start the session of the spec at once. " +
      "The result gives the repo, the slug and the folder of the new spec. " +
      "This tool fails at once when the Mac is offline.",
    inputSchema: {
      repo,
      title: z.string().min(1).describe("The title of the spec."),
      brief: z.string().min(1).describe("The brief of the spec. It goes into spec.md."),
      mode: z
        .enum(["autopilot", "guided"])
        .optional()
        .describe("The mode of the spec. Autopilot is the default."),
      remote_control: z
        .boolean()
        .optional()
        .describe("Turn Remote Control on or off for the session of the spec. On is the default."),
      start: z.boolean().optional().describe("Start the session of the spec at once. Off is the default."),
    },
  },
  {
    name: "start_spec",
    title: "Start a spec",
    description:
      "Start the session of an open spec. Spec Studio opens when it does not run. " +
      "When the session of the spec runs, no second session starts, and the result says so. " +
      "This tool fails at once when the Mac is offline.",
    inputSchema: { repo, slug },
  },
];

export const TOOL_NAMES = TOOLS.map((tool) => tool.name);

/**
 * The tools that the relay answers itself, from its store, with the Mac online
 * or offline. The relay registers them only when it has a store.
 */
export const LOCAL_TOOLS: ToolDefinition[] = [
  {
    name: "send_message",
    title: "Send a message to a spec",
    description:
      "Send a message to the session of a spec. The relay keeps the message until the Mac takes it. " +
      "Spec Studio then types the message into the terminal of the spec at a safe point. " +
      "The result gives the sequence number of the message at once, also when the Mac is offline. " +
      "A message does not answer a question or a permission prompt of the session.",
    inputSchema: {
      repo,
      slug,
      text: z.string().min(1).max(20_000).describe("The text of the message, in Markdown."),
      replyTo: z.string().min(1).optional().describe("The ID of the message that this message answers."),
      attachments: z
        .array(z.string().min(1))
        .max(10)
        .optional()
        .describe("The object keys of files that upload_attachment gave for this spec."),
    },
  },
  {
    name: "read_messages",
    title: "Read the messages from specs",
    description:
      "Read the messages that the sessions of specs sent with studio message. " +
      "With a slug, the result gives the messages of that spec after the cursor, with the stage of the spec. " +
      "Keep the cursor of the result and give it as after in the next call. " +
      "Without a slug, the result gives the unread messages of all specs. " +
      "When no message is there, the call waits up to 20 seconds for a new message.",
    inputSchema: {
      repo: repo.optional().describe("The path of the repo on the Mac. Give it with the slug, or alone to read one repo."),
      slug: slug.optional(),
      after: z
        .number()
        .int()
        .min(0)
        .optional()
        .describe("The cursor: the sequence number of the last message that you read. Use it only with a slug."),
    },
  },
  {
    name: "upload_attachment",
    title: "Upload an attachment",
    description:
      "Upload a file for a message to a spec. Give the contents as base64. " +
      "The result gives the object key of the file. Give the object key in the attachments of send_message. " +
      "The relay refuses a file above its size limit, and the result gives the limit. " +
      "Attachments of a message for Claude AI show a link that works for 15 minutes.",
    inputSchema: {
      repo,
      slug,
      filename: z.string().min(1).max(200).describe("The name of the file, for example screenshot.png."),
      contentType: z.string().min(1).max(200).describe("The media type of the file, for example image/png."),
      base64: z.string().min(1).describe("The contents of the file, in base64."),
    },
  },
];
