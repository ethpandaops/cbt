import { type JSX } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { listAllModelsOptions } from '@api/@tanstack/react-query.gen';
import { CircleStackIcon, TableCellsIcon, ArchiveBoxIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { ArrowPathIcon } from '@heroicons/react/24/solid';

export function ModelsList(): JSX.Element {
  const { data, error, isLoading } = useQuery(listAllModelsOptions());

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
          <Link
            key={model.id}
            to="/model/$model"
            params={{ model: model.id }}
            id={`model-${model.id}`}
            className="group relative overflow-hidden rounded-lg border border-gray-200 bg-white p-5 shadow-sm transition-all hover:border-indigo-300 hover:shadow-md"
          >
            <div className="space-y-3">
              <div className="flex items-start justify-between gap-2">
                <h3
                  className="break-all font-mono text-sm font-semibold text-gray-900 group-hover:text-indigo-600"
                  title={model.id}
                >
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

              {model.description && (
                <div className="border-t border-gray-100 pt-3">
                  <p className="text-xs text-gray-600">{model.description}</p>
                </div>
              )}
            </div>
          </Link>
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
