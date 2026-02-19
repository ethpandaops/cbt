import { type JSX, useState } from 'react';
import { PrismLight as SyntaxHighlighter } from 'react-syntax-highlighter';
import sql from 'react-syntax-highlighter/dist/esm/languages/prism/sql';
import { oneLight, vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { DocumentDuplicateIcon, CheckIcon } from '@heroicons/react/24/outline';
import { useTheme } from '@/hooks/useTheme';

// Register only SQL language to reduce bundle size
SyntaxHighlighter.registerLanguage('sql', sql);

export interface SQLCodeBlockProps {
  sql: string;
  title?: string;
}

export function SQLCodeBlock({ sql, title = 'SQL Query' }: SQLCodeBlockProps): JSX.Element {
  const [copied, setCopied] = useState(false);
  const { theme } = useTheme();

  const handleCopy = async (): Promise<void> => {
    await navigator.clipboard.writeText(sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="overflow-hidden rounded-xl border border-incremental/45 bg-linear-to-br from-surface/95 via-surface/86 to-secondary/30 shadow-lg ring-1 ring-border/50">
      <div className="flex items-center justify-between border-b border-border/55 bg-surface/75 px-4 py-2">
        <span className="text-sm font-semibold text-foreground">{title}</span>
        <button
          onClick={handleCopy}
          className="flex items-center gap-1.5 rounded-md bg-secondary/70 px-2.5 py-1.5 text-xs font-semibold text-primary ring-1 ring-border/45 transition-all hover:bg-secondary hover:text-accent hover:ring-accent/35"
          title="Copy to clipboard"
        >
          {copied ? (
            <>
              <CheckIcon className="size-4 text-success" />
              <span className="text-success">Copied!</span>
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
          style={theme === 'dark' ? vscDarkPlus : oneLight}
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
