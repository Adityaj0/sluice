import MonacoEditor from "@monaco-editor/react";
import type { OnMount } from "@monaco-editor/react";
import { useEffect, useRef } from "react";

import { LANGUAGE, THEME, monaco, registerFlowSql } from "../../lib/monaco.ts";
import type { SqlError } from "../../types/pipeline.ts";

registerFlowSql();

type StandaloneEditor = Parameters<OnMount>[0];

/** Monaco with the streaming SQL dialect; syntax errors are underlined. */
export function Editor({ value, onChange, error }: { value: string; onChange: (sql: string) => void; error: SqlError | null }) {
  const editorRef = useRef<StandaloneEditor | null>(null);

  useEffect(() => {
    const model = editorRef.current?.getModel();
    if (!model) return;
    const markers =
      error?.kind === "syntax"
        ? [
            {
              severity: monaco.MarkerSeverity.Error,
              message: error.message,
              startLineNumber: error.line,
              startColumn: error.column,
              endLineNumber: error.line,
              endColumn: error.column + Math.max(1, model.getWordAtPosition({ lineNumber: error.line, column: error.column })?.word.length ?? 1),
            },
          ]
        : [];
    monaco.editor.setModelMarkers(model, "sluice", markers);
  }, [error]);

  return (
    <MonacoEditor
      language={LANGUAGE}
      theme={THEME}
      value={value}
      onChange={(v) => onChange(v ?? "")}
      onMount={(editor) => {
        editorRef.current = editor;
      }}
      options={{
        fontSize: 14,
        fontFamily: "'JetBrains Mono Variable', ui-monospace, Menlo, monospace",
        lineHeight: 22,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        lineNumbersMinChars: 3,
        padding: { top: 16 },
        scrollbar: { verticalScrollbarSize: 8, horizontalScrollbarSize: 8 },
        overviewRulerLanes: 0,
        renderLineHighlight: "line",
        wordWrap: "on",
        tabSize: 2,
      }}
    />
  );
}
