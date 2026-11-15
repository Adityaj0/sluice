// Monaco, bundled locally (no CDN), with Sluice's streaming SQL dialect.

import { loader } from "@monaco-editor/react";
import * as monaco from "monaco-editor";
import EditorWorker from "monaco-editor/editor/editor.worker.js?worker";

(globalThis as { MonacoEnvironment?: unknown }).MonacoEnvironment = {
  getWorker: () => new EditorWorker(),
};
loader.config({ monaco });

const KEYWORDS = [
  "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "AS", "AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE",
  "WINDOW", "TUMBLING", "SLIDING", "SESSION", "SIZE", "SLIDE", "GAP", "ALLOWED", "LATENESS", "EMIT",
];
const UNITS = ["MILLISECONDS", "MILLISECOND", "MS", "SECONDS", "SECOND", "MINUTES", "MINUTE", "HOURS", "HOUR", "DAYS", "DAY"];
const FUNCTIONS = ["COUNT", "SUM", "AVG", "MIN", "MAX", "LOWER", "UPPER", "LENGTH", "ABS", "ROUND", "COALESCE"];
const PSEUDO = ["WINDOW_START", "WINDOW_END"];

export const LANGUAGE = "flowsql";
export const THEME = "sluice-light";

let registered = false;

export function registerFlowSql() {
  if (registered) return;
  registered = true;
  monaco.languages.register({ id: LANGUAGE });
  monaco.languages.setLanguageConfiguration(LANGUAGE, {
    comments: { lineComment: "--" },
    brackets: [["(", ")"]],
    autoClosingPairs: [
      { open: "(", close: ")" },
      { open: "'", close: "'" },
      { open: '"', close: '"' },
    ],
  });
  monaco.languages.setMonarchTokensProvider(LANGUAGE, {
    ignoreCase: true,
    keywords: KEYWORDS,
    units: UNITS,
    functions: FUNCTIONS,
    pseudo: PSEUDO,
    tokenizer: {
      root: [
        [/--.*$/, "comment"],
        [/'([^']|'')*'/, "string"],
        [/"[^"]*"/, "identifier.quoted"],
        [/\d+(\.\d+)?/, "number"],
        [
          /[A-Za-z_][\w]*/,
          {
            cases: {
              "@pseudo": "variable.predefined",
              "@functions": "predefined",
              "@keywords": "keyword",
              "@units": "type",
              "@default": "identifier",
            },
          },
        ],
        [/[<>!=]=?|<>|[+\-*/%]/, "operator"],
        [/[(),.;]/, "delimiter"],
      ],
    },
  } as monaco.languages.IMonarchLanguage);

  monaco.editor.defineTheme(THEME, {
    base: "vs",
    inherit: true,
    rules: [
      { token: "keyword", foreground: "4f46e5", fontStyle: "bold" },
      { token: "predefined", foreground: "7c3aed" },
      { token: "variable.predefined", foreground: "db2777" },
      { token: "type", foreground: "b45309" },
      { token: "number", foreground: "b45309" },
      { token: "string", foreground: "047857" },
      { token: "comment", foreground: "94a3b8", fontStyle: "italic" },
      { token: "operator", foreground: "64748b" },
      { token: "identifier", foreground: "0f172a" },
    ],
    colors: {
      "editor.background": "#ffffff",
      "editor.lineHighlightBackground": "#f8fafc",
      "editor.lineHighlightBorder": "#f8fafc",
      "editorLineNumber.foreground": "#cbd2d9",
      "editorLineNumber.activeForeground": "#5f7281",
      "editorGutter.background": "#ffffff",
      "editorIndentGuide.background1": "#eff1f4",
    },
  });

  monaco.languages.registerCompletionItemProvider(LANGUAGE, {
    provideCompletionItems(model, position) {
      const word = model.getWordUntilPosition(position);
      const range = new monaco.Range(position.lineNumber, word.startColumn, position.lineNumber, word.endColumn);
      const { Keyword, Function, Snippet, Variable } = monaco.languages.CompletionItemKind;
      const snippet = monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet;
      return {
        suggestions: [
          ...KEYWORDS.map((k) => ({ label: k, kind: Keyword, insertText: k, range })),
          ...FUNCTIONS.map((f) => ({ label: f, kind: Function, insertText: `${f}($1)`, insertTextRules: snippet, range })),
          ...PSEUDO.map((p) => ({ label: p, kind: Variable, insertText: p, range })),
          { label: "WINDOW TUMBLING", kind: Snippet, insertText: "WINDOW TUMBLING (SIZE ${1:1} ${2:MINUTE})", insertTextRules: snippet, range },
          {
            label: "WINDOW SLIDING",
            kind: Snippet,
            insertText: "WINDOW SLIDING (SIZE ${1:10} ${2:MINUTES}, SLIDE ${3:1} ${4:MINUTE})",
            insertTextRules: snippet,
            range,
          },
          { label: "WINDOW SESSION", kind: Snippet, insertText: "WINDOW SESSION (GAP ${1:30} ${2:SECONDS})", insertTextRules: snippet, range },
          { label: "ALLOWED LATENESS", kind: Snippet, insertText: "ALLOWED LATENESS ${1:10} ${2:SECONDS}", insertTextRules: snippet, range },
        ],
      };
    },
  });
}

export { monaco };
