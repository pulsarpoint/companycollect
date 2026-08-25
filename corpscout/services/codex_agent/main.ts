import { Codex } from "@openai/codex-sdk";

const codex = new Codex();
const agent = codex.startThread();

const result = await agent.run(
  "Inspect this project and briefly explain what it does. Do not change any files."
);

console.log(result.finalResponse);