import { type JSX } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  getExternalModelOptions,
  getExternalBoundsOptions,
  listExternalBoundsOptions,
  getTransformationOptions,
  listTransformationCoverageOptions,
  listTransformationsOptions,
  getIntervalTypesOptions,
} from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { BackToDashboardButton } from '@/components/BackToDashboardButton';
import { ModelHeader } from '@/components/ModelHeader';
import { ModelInfoCard, type InfoField } from '@/components/ModelInfoCard';
import { ModelDetailView } from '@/components/ModelDetailView';
import { SQLCodeBlock } from '@/components/SQLCodeBlock';
import { timeAgo } from '@/utils/time';

function ModelDetailComponent(): JSX.Element {
  const { id } = Route.useParams();
  const decodedId = decodeURIComponent(id);

  // Load all models to determine type
  const allModels = useQuery(listAllModelsOptions());
  const model = allModels.data?.models.find(m => m.id === decodedId);

  // Fetch interval types for transformation selector
  const intervalTypes = useQuery(getIntervalTypesOptions());

  // Conditional queries based on model type
  const externalModel = useQuery({
    ...getExternalModelOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'external',
  });

  const externalBounds = useQuery({
    ...getExternalBoundsOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'external',
  });

  const transformationModel = useQuery({
    ...getTransformationOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'transformation',
  });

  const coverage = useQuery(listTransformationCoverageOptions());
  const allBounds = useQuery({
    ...listExternalBoundsOptions(),
    enabled: model?.type === 'transformation',
  });

  // Fetch all transformations for recursive dependency resolution (polling handled at root level)
  const allTransformations = useQuery({
    ...listTransformationsOptions(),
    enabled: model?.type === 'transformation',
  });

  if (allModels.isLoading) {
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin" />
        <span>Loading...</span>
      </div>
    );
  }

  if (!model) {
    return (
      <div className="rounded-lg border border-red-500/50 bg-red-950/80 p-4 text-red-200">
        <div className="flex items-center gap-2">
          <XCircleIcon className="h-5 w-5 shrink-0" />
          <span className="font-medium">Model not found: {decodedId}</span>
        </div>
      </div>
    );
  }

  if (model.type === 'external') {
    if (externalModel.isLoading || externalBounds.isLoading) {
      return (
        <div className="flex items-center gap-3 text-slate-400">
          <ArrowPathIcon className="h-5 w-5 animate-spin" />
          <span>Loading external model details...</span>
        </div>
      );
    }

    const bounds = externalBounds.data;
    const extModel = externalModel.data;

    const fields: InfoField[] = [
      { label: 'Database', value: model.database },
      { label: 'Table', value: model.table },
    ];

    if (extModel?.description) {
      fields.push({ label: 'Description', value: extModel.description });
    }

    if (extModel?.interval?.type) {
      // Use transformation name if available, otherwise fall back to interval.type
      const intervalType = extModel.interval.type;
      const transformation = intervalTypes.data?.interval_types?.[intervalType]?.[0];
      fields.push({ label: 'Interval Type', value: transformation?.name || intervalType });
    }

    if (bounds) {
      fields.push(
        { label: 'Min Position', value: bounds.min.toLocaleString(), variant: 'highlight', highlightColor: 'green' },
        { label: 'Max Position', value: bounds.max.toLocaleString(), variant: 'highlight', highlightColor: 'green' }
      );
    }

    return (
      <div className="space-y-6">
        <ModelHeader modelId={decodedId} modelType="external" />
        <BackToDashboardButton />
        <ModelInfoCard title="Model Information" fields={fields} borderColor="border-green-500/30" />
      </div>
    );
  }

  // Transformation model
  if (transformationModel.isLoading || coverage.isLoading) {
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin" />
        <span>Loading transformation model details...</span>
      </div>
    );
  }

  const transformation = transformationModel.data;
  const isIncremental = transformation?.type === 'incremental';

  if (!isIncremental) {
    // Scheduled transformation - no coverage
    const fields: InfoField[] = [
      { label: 'Database', value: model.database },
      { label: 'Table', value: model.table },
      { label: 'Content Type', value: transformation?.content_type, variant: 'highlight', highlightColor: 'indigo' },
    ];

    if (transformation?.description) {
      fields.push({ label: 'Description', value: transformation.description });
    }

    if (transformation?.schedule) {
      fields.push({
        label: 'Schedule',
        value: transformation.schedule,
        variant: 'highlight',
        highlightColor: 'emerald',
      });
    }

    if (transformation?.metadata?.last_run_at) {
      fields.push({ label: 'Last Run', value: timeAgo(transformation.metadata.last_run_at) });
    }

    if (transformation?.metadata?.last_run_status) {
      fields.push({ label: 'Status', value: transformation.metadata.last_run_status });
    }

    if (transformation?.tags && transformation.tags.length > 0) {
      fields.push({ label: 'Tags', value: transformation.tags.join(', ') });
    }

    if (transformation?.depends_on && transformation.depends_on.length > 0) {
      fields.push({ label: 'Dependencies', value: `${transformation.depends_on.length} model(s)` });
    }

    return (
      <div className="space-y-6">
        <ModelHeader modelId={decodedId} modelType="scheduled" />
        <BackToDashboardButton />
        <ModelInfoCard title="Transformation Details" fields={fields} borderColor="border-emerald-500/30" />

        {transformation?.content && transformation.content_type === 'sql' && (
          <SQLCodeBlock sql={transformation.content} title="Transformation Query" />
        )}

        {transformation?.content && transformation.content_type === 'exec' && (
          <div className="overflow-hidden rounded-lg border border-emerald-500/30 bg-slate-900/80 shadow-lg">
            <div className="border-b border-slate-700/50 bg-slate-800/60 px-4 py-2">
              <span className="text-sm font-semibold text-slate-300">Execution Command</span>
            </div>
            <div className="p-4">
              <pre className="overflow-auto text-sm">
                <code className="font-mono text-slate-200">{transformation.content}</code>
              </pre>
            </div>
          </div>
        )}
      </div>
    );
  }

  // Incremental transformation - use separate component
  return (
    <div>
      <ModelHeader modelId={decodedId} modelType="incremental" />
      <BackToDashboardButton />
      <ModelDetailView
        decodedId={decodedId}
        transformation={transformation}
        coverage={coverage}
        allBounds={allBounds}
        allTransformations={allTransformations}
        intervalTypes={intervalTypes}
      />
    </div>
  );
}

export const Route = createFileRoute('/model/$id')({
  component: ModelDetailComponent,
});
