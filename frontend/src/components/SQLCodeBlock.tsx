import { type JSX, useState } from 'react';
import { PrismLight as SyntaxHighlighter } from 'react-syntax-highlighter';
import sql from 'react-syntax-highlighter/dist/esm/languages/prism/sql';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { DocumentDuplicateIcon, CheckIcon } from '@heroicons/react/24/outline';

// Register only SQL language to reduce bundle size
SyntaxHighlighter.registerLanguage('sql', sql);

export interface SQLCodeBlockProps {
  sql: string;
  title?: string;
}

export function SQLCodeBlock({ sql, title = 'SQL Query' }: SQLCodeBlockProps): JSX.Element {
  const [copied, setCopied] = useState(false);

  const handleCopy = async (): Promise<void> => {
    await navigator.clipboard.writeText(sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="overflow-hidden rounded-lg border border-indigo-500/30 bg-slate-900/80 shadow-lg">
      <div className="flex items-center justify-between border-b border-slate-700/50 bg-slate-800/60 px-4 py-2">
        <span className="text-sm font-semibold text-slate-300">{title}</span>
        <button
          onClick={handleCopy}
          className="flex items-center gap-1.5 rounded-md bg-slate-700/50 px-2.5 py-1.5 text-xs font-medium text-slate-300 transition-all hover:bg-slate-700 hover:text-slate-100"
          title="Copy to clipboard"
        >
          {copied ? (
            <>
              <CheckIcon className="size-4 text-green-400" />
              <span className="text-green-400">Copied!</span>
            </>
          ) : (
            <>
              <DocumentDuplicateIcon className="size-4" />
              <span>Copy</span>
            </>
          )}
        </button>
      </div>
      <div className="max-h-96 overflow-auto p-4">
        <SyntaxHighlighter
          language="sql"
          style={vscDarkPlus}
          customStyle={{
            background: 'transparent',
            padding: 0,
            margin: 0,
            fontSize: '0.875rem',
            lineHeight: '1.5',
          }}
          wrapLongLines
        >
          {sql}
        </SyntaxHighlighter>
      </div>
    </div>
  );
}
