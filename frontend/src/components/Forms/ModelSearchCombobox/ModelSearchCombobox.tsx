import { type JSX, useState, Fragment } from 'react';
import { Combobox, Transition } from '@headlessui/react';
import { MagnifyingGlassIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { useNavigate } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { listAllModelsOptions } from '@/api/@tanstack/react-query.gen';
import type { ModelSummary } from '@/api/types.gen';
import { TypeBadge } from '@/components/Elements/TypeBadge';
import type { ModelType } from '@/types';

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

  const getModelType = (model: ModelSummary): ModelType => {
    if (model.type === 'external') return 'external';
    // For transformations, default to incremental (we don't have enough info to determine scheduled)
    return 'incremental';
  };

  if (variant === 'icon') {
    return (
      <Combobox value={selectedModel} onChange={handleModelSelect} by="id">
        <div className="relative">
          <Combobox.Button className="glass-icon-control text-foreground">
            <MagnifyingGlassIcon className="size-5" />
          </Combobox.Button>

          <Transition
            as={Fragment}
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100"
            leaveTo="opacity-0"
            afterLeave={() => setQuery('')}
          >
            <Combobox.Options className="glass-surface fixed inset-x-4 top-24 z-[100] max-h-96 overflow-auto">
              <div className="sticky top-0 border-b border-border/50 bg-surface/90 px-4 py-3 backdrop-blur-xl">
                <div className="relative">
                  <MagnifyingGlassIcon className="pointer-events-none absolute top-1/2 left-3 size-5 -translate-y-1/2 text-muted" />
                  <Combobox.Input
                    className="w-full rounded-lg border border-border/65 bg-surface/92 py-2.5 pr-10 pl-10 text-sm/6 text-foreground placeholder-muted transition-all focus:border-accent focus:ring-2 focus:ring-accent/45 focus:outline-hidden"
                    placeholder="Search models..."
                    onChange={e => setQuery(e.target.value)}
                    displayValue={(model: ModelSummary) => model?.id || ''}
                  />
                  {query && (
                    <button
                      onClick={() => setQuery('')}
                      className="absolute top-1/2 right-3 -translate-y-1/2 text-muted hover:text-foreground"
                    >
                      <XMarkIcon className="size-4" />
                    </button>
                  )}
                </div>
              </div>

              {isLoading ? (
                <div className="px-4 py-8 text-center text-sm/6 text-muted">Loading models...</div>
              ) : filteredModels.length === 0 ? (
                <div className="px-4 py-8 text-center text-sm/6 text-muted">
                  {query ? 'No models found.' : 'No models available.'}
                </div>
              ) : (
                <div>
                  {filteredModels.map(model => (
                    <Combobox.Option
                      key={model.id}
                      value={model}
                      className={({ active }) =>
                        `cursor-pointer px-4 py-3 transition-all select-none ${
                          active ? 'bg-secondary/75 text-primary' : 'text-foreground'
                        }`
                      }
                    >
                      {({ active }) => (
                        <div className="flex items-start gap-3">
                          <MagnifyingGlassIcon
                            className={`mt-0.5 size-4 shrink-0 ${active ? 'text-accent' : 'text-muted'}`}
                          />
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-2">
                              <span className="truncate font-mono text-sm/6 font-semibold">{model.id}</span>
                              <TypeBadge type={getModelType(model)} compact />
                            </div>
                            {model.description && (
                              <p className="mt-1 truncate text-xs text-muted">{model.description}</p>
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
          <MagnifyingGlassIcon className="pointer-events-none absolute top-1/2 left-3 size-5 -translate-y-1/2 text-muted" />
          <Combobox.Input
            className="w-full rounded-lg border border-border/65 bg-surface/92 py-2.5 pr-10 pl-10 text-base/6 text-foreground placeholder-muted shadow-xs ring-1 ring-border/40 transition-all focus:border-accent focus:ring-2 focus:ring-accent/45 focus:outline-hidden"
            placeholder="Search models..."
            onChange={e => setQuery(e.target.value)}
            displayValue={(model: ModelSummary) => model?.id || ''}
          />
          {query && (
            <button
              onClick={() => setQuery('')}
              className="absolute top-1/2 right-3 -translate-y-1/2 text-muted hover:text-foreground"
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
          <Combobox.Options className="glass-surface absolute z-[100] mt-2 max-h-96 w-full overflow-auto">
            {isLoading ? (
              <div className="px-4 py-8 text-center text-sm/6 text-muted">Loading models...</div>
            ) : filteredModels.length === 0 ? (
              <div className="px-4 py-8 text-center text-sm/6 text-muted">
                {query ? 'No models found.' : 'No models available.'}
              </div>
            ) : (
              <div>
                {filteredModels.map(model => (
                  <Combobox.Option
                    key={model.id}
                    value={model}
                    className={({ active }) =>
                      `cursor-pointer px-4 py-3 transition-all select-none ${
                        active ? 'bg-secondary/75 text-primary' : 'text-foreground'
                      }`
                    }
                  >
                    {({ active }) => (
                      <div className="flex items-start gap-3">
                        <MagnifyingGlassIcon
                          className={`mt-0.5 size-4 shrink-0 ${active ? 'text-accent' : 'text-muted'}`}
                        />
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="truncate font-mono text-sm/6 font-semibold">{model.id}</span>
                            <TypeBadge type={getModelType(model)} compact />
                          </div>
                          {model.description && <p className="mt-1 truncate text-xs text-muted">{model.description}</p>}
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
