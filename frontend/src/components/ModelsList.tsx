import { type JSX, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getModelsOptions } from '@api/@tanstack/react-query.gen';
import { CircleStackIcon, TableCellsIcon, ArchiveBoxIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { ArrowPathIcon } from '@heroicons/react/24/solid';

export function ModelsList(): JSX.Element {
  const { data, error, isLoading } = useQuery(getModelsOptions());
  const [hoveredDep, setHoveredDep] = useState<string | null>(null);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="flex items-center gap-3 text-gray-600">
          <ArrowPathIcon className="h-5 w-5 animate-spin" />
          <span>Loading models...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-red-800">
        <div className="flex items-center gap-2">
          <XCircleIcon className="h-5 w-5 shrink-0" />
          <span className="font-medium">Error: {error.message}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-gray-900">
          Models
          <span className="ml-2 text-sm font-normal text-gray-500">({data?.total})</span>
        </h2>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {data?.models.map(model => (
          <div
            key={model.id}
            id={`model-${model.id}`}
            className={`group relative overflow-hidden rounded-lg border p-5 shadow-sm transition-all ${
              hoveredDep === model.id
                ? 'border-indigo-400 bg-indigo-50 shadow-lg ring-2 ring-indigo-400 ring-offset-2'
                : 'border-gray-200 bg-white hover:border-gray-300 hover:shadow-md'
            }`}
          >
            <div className="space-y-3">
              <div className="flex items-start justify-between gap-2">
                <h3 className="break-all font-mono text-sm font-semibold text-gray-900" title={model.id}>
                  {model.id}
                </h3>
                <span className="inline-flex shrink-0 items-center rounded-full bg-indigo-100 px-2.5 py-0.5 text-xs font-medium text-indigo-800">
                  {model.type}
                </span>
              </div>

              <div className="space-y-1.5 text-sm text-gray-600">
                <div className="flex items-center gap-2">
                  <CircleStackIcon className="h-4 w-4 shrink-0 text-gray-400" />
                  <span className="truncate font-medium" title={model.database}>
                    {model.database}
                  </span>
                </div>

                <div className="flex items-center gap-2">
                  <TableCellsIcon className="h-4 w-4 shrink-0 text-gray-400" />
                  <span className="truncate" title={model.table}>
                    {model.table}
                  </span>
                </div>
              </div>

              {model.dependencies && model.dependencies.length > 0 && (
                <div className="space-y-1 border-t border-gray-100 pt-3">
                  <div className="text-xs font-medium uppercase tracking-wide text-gray-500">Dependencies</div>
                  <div className="flex flex-wrap gap-1">
                    {model.dependencies.map(dep => (
                      <span
                        key={dep}
                        className="inline-flex cursor-pointer items-center rounded-md bg-gray-100 px-2 py-1 font-mono text-xs text-gray-700 transition-all hover:border-indigo-500 hover:bg-indigo-100 hover:text-indigo-800 hover:ring-2 hover:ring-indigo-500"
                        title={dep}
                        onMouseEnter={() => setHoveredDep(dep)}
                        onMouseLeave={() => setHoveredDep(null)}
                      >
                        <span className="max-w-[150px] truncate">{dep}</span>
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>

      {data?.models.length === 0 && (
        <div className="rounded-lg border-2 border-dashed border-gray-300 p-12 text-center">
          <ArchiveBoxIcon className="mx-auto h-12 w-12 text-gray-400" />
          <h3 className="mt-2 text-sm font-medium text-gray-900">No models found</h3>
          <p className="mt-1 text-sm text-gray-500">Get started by creating a new model.</p>
        </div>
      )}
    </div>
  );
}
