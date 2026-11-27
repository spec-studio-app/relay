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

/** The slug of the tools that also go to a desk: no slug means the desk of the repo. */
const specOrDesk = z
  .string()
  .min(1)
  .optional()
  .describe("The slug of the spec. Use a slug that list_specs gives. Give no slug for the desk of the repo.");

export const TOOLS: ToolDefinition[] = [
  {
    name: "list_repos",
    title: "List the repos",
    description:
      "List the repos in Spec Studio, in sidebar order. Each repo shows its path on the Mac. Use the path as the repo argument of the other tools. " +
      "Each repo also shows its desk: open or closed, its turn, and whether Claude AI can message the desk. " +
      "When the Mac is offline, the list comes from the last connection of the Mac, and the result gives its time. " +
      "Each desk then shows the time of its last state. " +
      "Each repo also shows its area.",
    inputSchema: {
      area: z.string().min(1).optional().describe("Show only the repos of this area. Give the name of the area."),
    },
  },
  {
    name: "list_specs",
    title: "List the specs",
    description:
      "List the open specs of Spec Studio. Each spec shows its title, slug, mode, stage, status, note and turn. " +
      "The list also shows whether the spec needs the user. " +
      "When Spec Studio does not run, the turn comes from state.json, and the result says so. " +
      "Each spec also shows the area of its repo and its project. " +
      "When the Mac is offline, the list comes from the relay. Each spec then shows the time of its last sync. " +
      "The relay has no areas and no projects, so an offline list shows all specs and says that the filter needs the Mac online.",
    inputSchema: {
      repo: repo.optional().describe("Show only the specs of this repo. Give the path of the repo on the Mac."),
      all: z.boolean().optional().describe("Also show the specs that are merged or closed."),
      area: z.string().min(1).optional().describe("Show only the specs of the repos of this area. Give the name of the area."),
      project: z.string().min(1).optional().describe("Show only the specs of this project. Give the name of the project."),
    },
  },
  {
    name: "list_projects",
    title: "List the projects",
    description:
      "List the projects of Spec Studio. A project is a goal with specs in one or more repos. " +
      "Each project shows its status, its progress (the specs that shipped of all its specs) and its target date. " +
      "Each project also shows its newest update, with its health and its note. " +
      "This tool fails at once when the Mac is offline.",
    inputSchema: {},
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
      "Give a project to put the new spec into that project. " +
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
      project: z
        .string()
        .min(1)
        .optional()
        .describe("The name of a project. The new spec goes into this project. Use a name that list_projects gives."),
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

/** The start of the answer of an older agent that does not know list_projects. */
export const UNKNOWN_LIST_PROJECTS_PREFIX = 'The tool "list_projects" does not exist';
export const UPDATE_FOR_PROJECTS_TEXT = "Update Spec Studio on the Mac to list projects.";

export const TOOL_NAMES = TOOLS.map((tool) => tool.name);

/**
 * The tools that the relay answers itself, from its store, with the Mac online
 * or offline. The relay registers them only when it has a store.
 */
export const LOCAL_TOOLS: ToolDefinition[] = [
  {
    name: "send_message",
    title: "Send a message to a spec or a desk",
    description:
      "Send a message to the session of a spec, or to the desk of a repo. " +
      "Give the repo and the slug for a spec. Give only the repo for the desk of the repo. " +
      "A desk is the session of a repo with no spec. " +
      "The relay keeps the message until the Mac takes it. " +
      "Spec Studio then types the message into the terminal at a safe point. " +
      "The user must let Claude AI message a desk in Spec Studio, Settings, Remote Relay. " +
      "The result gives the sequence number of the message at once, also when the Mac is offline. " +
      "A message does not answer a question or a permission prompt of the session.",
    inputSchema: {
      repo,
      slug: specOrDesk,
      text: z.string().min(1).max(20_000).describe("The text of the message, in Markdown."),
      replyTo: z.string().min(1).optional().describe("The ID of the message that this message answers."),
      attachments: z
        .array(z.string().min(1))
        .max(10)
        .optional()
        .describe("The object keys of files that upload_attachment gave for this spec or desk."),
    },
  },
  {
    name: "read_messages",
    title: "Read the messages from specs and desks",
    description:
      "Read the messages that the sessions of specs and the desks of repos sent with studio message. " +
      "With a repo and a slug, the result gives the messages of that spec after the cursor, with the stage of the spec. " +
      "With only a repo, the result gives the messages of the desk of that repo after the cursor. " +
      "Keep the cursor of the result and give it as after in the next call. " +
      "With no repo and no slug, the result gives the unread messages of all specs and all desks. " +
      "A message from a desk shows its repo and desk true. " +
      "A message from the user is the user's answer in the terminal of the spec, which Spec Studio copied. " +
      "For a spec, the result also gives its latest status change (stage, status, note and time) and its turn. " +
      "When the spec changed after the last message, the result says so in stale: then use read_spec. " +
      "When no message is there, the call waits up to 20 seconds for a new message.",
    inputSchema: {
      repo: repo
        .optional()
        .describe("The path of the repo on the Mac. Give it with the slug for a spec, or alone for the desk of the repo."),
      slug: specOrDesk,
      after: z
        .number()
        .int()
        .min(0)
        .optional()
        .describe("The cursor: the sequence number of the last message that you read. Use it with a repo."),
    },
  },
  {
    name: "upload_attachment",
    title: "Upload an attachment",
    description:
      "Upload a file for a message to a spec or to a desk. Give the contents as base64. " +
      "Give the repo and the slug for a spec, or only the repo for a desk. " +
      "The result gives the object key of the file. Give the object key in the attachments of send_message. " +
      "The relay refuses a file above its size limit, and the result gives the limit. " +
      "Attachments of a message for Claude AI show a link that works for 15 minutes.",
    inputSchema: {
      repo,
      slug: specOrDesk,
      filename: z.string().min(1).max(200).describe("The name of the file, for example screenshot.png."),
      contentType: z.string().min(1).max(200).describe("The media type of the file, for example image/png."),
      base64: z.string().min(1).describe("The contents of the file, in base64."),
    },
  },
];
