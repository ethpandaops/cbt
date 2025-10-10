import { type JSX, useState, Fragment } from 'react';
import { Combobox, Transition } from '@headlessui/react';
import { MagnifyingGlassIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { useNavigate } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { listAllModelsOptions } from '@api/@tanstack/react-query.gen';
import type { ModelSummary } from '@api/types.gen';

export interface ModelSearchComboboxProps {
  /**
   * Show full search bar (desktop) or icon only (mobile)
   */
  variant?: 'full' | 'icon';
}

export function ModelSearchCombobox({ variant = 'full' }: ModelSearchComboboxProps): JSX.Element {
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const [selectedModel, setSelectedModel] = useState<ModelSummary | null>(null);

  // Fetch all models for search
  const { data, isLoading } = useQuery(listAllModelsOptions());

  const models = data?.models || [];

  // Filter models based on search query
  const filteredModels =
    query === ''
      ? models.slice(0, 10) // Show first 10 when no query
      : models.filter(model => {
          const searchTerm = query.toLowerCase();
          return (
            model.id.toLowerCase().includes(searchTerm) ||
            model.database.toLowerCase().includes(searchTerm) ||
            model.table.toLowerCase().includes(searchTerm) ||
            model.description?.toLowerCase().includes(searchTerm)
          );
        });

  const handleModelSelect = (model: ModelSummary | null): void => {
    setSelectedModel(model);
    if (model) {
      void navigate({ to: '/model/$id', params: { id: encodeURIComponent(model.id) } });
      // Reset after navigation
      setTimeout(() => {
        setQuery('');
        setSelectedModel(null);
      }, 100);
    }
  };

  const getModelTypeBadge = (model: ModelSummary): JSX.Element => {
    if (model.type === 'external') {
      return (
        <span className="inline-flex shrink-0 items-center rounded-md bg-green-500/20 px-2 py-0.5 text-xs font-semibold text-green-300 ring-1 ring-green-500/30">
          External
        </span>
      );
    }
    // For transformations, we don't know if incremental/scheduled without fetching more data
    return (
      <span className="inline-flex shrink-0 items-center rounded-md bg-indigo-500/20 px-2 py-0.5 text-xs font-semibold text-indigo-300 ring-1 ring-indigo-500/30">
        Transform
      </span>
    );
  };

  if (variant === 'icon') {
    return (
      <Combobox value={selectedModel} onChange={handleModelSelect} by="id">
        <div className="relative">
          <Combobox.Button className="flex items-center justify-center rounded-lg p-2 text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400">
            <MagnifyingGlassIcon className="size-5" />
          </Combobox.Button>

          <Transition
            as={Fragment}
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
            afterLeave={() => setQuery('')}
          >
            <Combobox.Options className="fixed inset-x-4 top-24 z-[100] max-h-96 overflow-auto rounded-xl border border-slate-700/50 bg-slate-900/95 shadow-2xl ring-1 ring-slate-700/50 backdrop-blur-xl">
              <div className="sticky top-0 border-b border-slate-700/50 bg-slate-900/95 px-4 py-3 backdrop-blur-xl">
                <div className="relative">
                  <MagnifyingGlassIcon className="pointer-events-none absolute left-3 top-1/2 size-5 -translate-y-1/2 text-slate-400" />
                  <Combobox.Input
                    className="w-full rounded-lg border border-slate-700/50 bg-slate-800/60 py-2.5 pl-10 pr-10 text-sm/6 text-slate-200 placeholder-slate-400 transition-all focus:border-indigo-500 focus:outline-hidden focus:ring-3 focus:ring-indigo-500/50"
                    placeholder="Search models..."
                    onChange={e => setQuery(e.target.value)}
                    displayValue={(model: ModelSummary) => model?.id || ''}
                  />
                  {query && (
                    <button
                      onClick={() => setQuery('')}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200"
                    >
                      <XMarkIcon className="size-4" />
                    </button>
                  )}
                </div>
              </div>

              {isLoading ? (
                <div className="px-4 py-8 text-center text-sm/6 text-slate-400">Loading models...</div>
              ) : filteredModels.length === 0 ? (
                <div className="px-4 py-8 text-center text-sm/6 text-slate-400">
                  {query ? 'No models found.' : 'No models available.'}
                </div>
              ) : (
                <div>
                  {filteredModels.map(model => (
                    <Combobox.Option
                      key={model.id}
                      value={model}
                      className={({ active }) =>
                        `cursor-pointer select-none px-4 py-3 transition-all ${
                          active ? 'bg-indigo-500/20 text-indigo-200' : 'text-slate-300'
                        }`
                      }
                    >
                      {({ active }) => (
                        <div className="flex items-start gap-3">
                          <MagnifyingGlassIcon
                            className={`mt-0.5 size-4 shrink-0 ${active ? 'text-indigo-400' : 'text-slate-500'}`}
                          />
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-2">
                              <span className="truncate font-mono text-sm/6 font-semibold">{model.id}</span>
                              {getModelTypeBadge(model)}
                            </div>
                            {model.description && (
                              <p className="mt-1 truncate text-xs text-slate-400">{model.description}</p>
                            )}
                          </div>
                        </div>
                      )}
                    </Combobox.Option>
                  ))}
                </div>
              )}
            </Combobox.Options>
          </Transition>
        </div>
      </Combobox>
    );
  }

  // Full variant (desktop)
  return (
    <Combobox value={selectedModel} onChange={handleModelSelect} by="id">
      <div className="relative w-full max-w-lg lg:max-w-xl">
        <div className="relative">
          <MagnifyingGlassIcon className="pointer-events-none absolute left-3 top-1/2 size-5 -translate-y-1/2 text-slate-400" />
          <Combobox.Input
            className="w-full rounded-lg border border-slate-700/50 bg-slate-800/60 py-2.5 pl-10 pr-10 text-base/6 text-slate-200 placeholder-slate-400 transition-all focus:border-indigo-500 focus:outline-hidden focus:ring-3 focus:ring-indigo-500/50"
            placeholder="Search models..."
            onChange={e => setQuery(e.target.value)}
            displayValue={(model: ModelSummary) => model?.id || ''}
          />
          {query && (
            <button
              onClick={() => setQuery('')}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200"
            >
              <XMarkIcon className="size-4" />
            </button>
          )}
        </div>

        <Transition
          as={Fragment}
          leave="transition ease-in duration-100"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
          afterLeave={() => setQuery('')}
        >
          <Combobox.Options className="absolute z-[100] mt-2 max-h-96 w-full overflow-auto rounded-xl border border-slate-700/50 bg-slate-900/95 shadow-2xl ring-1 ring-slate-700/50 backdrop-blur-xl">
            {isLoading ? (
              <div className="px-4 py-8 text-center text-sm/6 text-slate-400">Loading models...</div>
            ) : filteredModels.length === 0 ? (
              <div className="px-4 py-8 text-center text-sm/6 text-slate-400">
                {query ? 'No models found.' : 'No models available.'}
              </div>
            ) : (
              <div>
                {filteredModels.map(model => (
                  <Combobox.Option
                    key={model.id}
                    value={model}
                    className={({ active }) =>
                      `cursor-pointer select-none px-4 py-3 transition-all ${
                        active ? 'bg-indigo-500/20 text-indigo-200' : 'text-slate-300'
                      }`
                    }
                  >
                    {({ active }) => (
                      <div className="flex items-start gap-3">
                        <MagnifyingGlassIcon
                          className={`mt-0.5 size-4 shrink-0 ${active ? 'text-indigo-400' : 'text-slate-500'}`}
                        />
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="truncate font-mono text-sm/6 font-semibold">{model.id}</span>
                            {getModelTypeBadge(model)}
                          </div>
                          {model.description && (
                            <p className="mt-1 truncate text-xs text-slate-400">{model.description}</p>
                          )}
                        </div>
                      </div>
                    )}
                  </Combobox.Option>
                ))}
              </div>
            )}
          </Combobox.Options>
        </Transition>
      </div>
    </Combobox>
  );
}
