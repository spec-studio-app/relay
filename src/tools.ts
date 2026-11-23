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
      "List the repos in Spec Studio, in sidebar order. Each repo shows its path on the Mac. Use the path as the repo argument of the other tools.",
    inputSchema: {},
  },
  {
    name: "list_specs",
    title: "List the specs",
    description:
      "List the open specs of Spec Studio. Each spec shows its title, slug, mode, stage, status, note and turn. " +
      "The list also shows whether the spec needs the user. " +
      "When Spec Studio does not run, the turn comes from state.json, and the result says so.",
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
      "It also shows spec.md, plan.md when it exists, and the last 20 events of the activity.",
    inputSchema: { repo, slug },
  },
  {
    name: "make_spec",
    title: "Make a spec",
    description:
      "Make a new spec in a repo with a title and a brief. The brief goes into spec.md. " +
      "The mode is autopilot or guided. Remote Control is on when you do not give a value. " +
      "Set start to true to start the session of the spec at once. " +
      "The result gives the repo, the slug and the folder of the new spec.",
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
      "When the session of the spec runs, no second session starts, and the result says so.",
    inputSchema: { repo, slug },
  },
];

export const TOOL_NAMES = TOOLS.map((tool) => tool.name);
