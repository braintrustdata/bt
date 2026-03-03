declare module "braintrust" {
  export function Eval(
    name: string,
    evaluator: {
      experimentName?: string;
      data: () => { input: string; expected?: string }[];
      task: (input: string) => Promise<string> | string;
      scores: ((args: {
        output: string;
        expected?: string;
        input?: string;
      }) => { name: string; score: number })[];
    },
  ): Promise<void>;
}

declare module "esm-only-pkg" {
  export function hello(): string;
}
