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
import { BackToDashboardButton } from '@/components/Elements/BackToDashboardButton';
import { ErrorState } from '@/components/Feedback/ErrorState';
import { ModelHeader } from '@/components/Domain/Models/ModelHeader';
import { ModelInfoCard, type InfoField } from '@/components/Domain/Models/ModelInfoCard';
import { ModelDetailView } from '@/components/Domain/Models/ModelDetailView';
import { ModelSkeleton } from '@/components/Domain/Models/ModelSkeleton';
import { SQLCodeBlock } from '@/components/Elements/SQLCodeBlock';
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
    return <ModelSkeleton />;
  }

  if (!model) {
    return <ErrorState message={`Model not found: ${decodedId}`} />;
  }

  if (model.type === 'external') {
    if (externalModel.isLoading || externalBounds.isLoading) {
      return <ModelSkeleton />;
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
        { label: 'Min Position', value: bounds.min.toLocaleString(), variant: 'highlight', highlightColor: 'external' },
        { label: 'Max Position', value: bounds.max.toLocaleString(), variant: 'highlight', highlightColor: 'external' }
      );
    }

    return (
      <div className="space-y-6">
        <ModelHeader modelId={decodedId} modelType="external" />
        <BackToDashboardButton />
        <ModelInfoCard title="Model Information" fields={fields} borderColor="border-external/30" />
      </div>
    );
  }

  // Transformation model
  const transformation = transformationModel.data;
  const isIncremental = transformation?.type === 'incremental';

  // Check if transformation model is still loading - show skeleton to prevent flash
  if (
    transformationModel.isLoading ||
    (isIncremental &&
      (coverage.isLoading || allBounds.isLoading || allTransformations.isLoading || intervalTypes.isLoading))
  ) {
    return <ModelSkeleton />;
  }

  if (!isIncremental) {
    // Scheduled transformation - no coverage
    const fields: InfoField[] = [
      { label: 'Database', value: model.database },
      { label: 'Table', value: model.table },
      {
        label: 'Content Type',
        value: transformation?.content_type,
        variant: 'highlight',
        highlightColor: 'incremental',
      },
    ];

    if (transformation?.description) {
      fields.push({ label: 'Description', value: transformation.description });
    }

    if (transformation?.schedule) {
      fields.push({
        label: 'Schedule',
        value: transformation.schedule,
        variant: 'highlight',
        highlightColor: 'scheduled',
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
        <ModelInfoCard title="Transformation Details" fields={fields} borderColor="border-scheduled/30" />

        {transformation?.content && transformation.content_type === 'sql' && (
          <SQLCodeBlock sql={transformation.content} title="Transformation Query" />
        )}

        {transformation?.content && transformation.content_type === 'exec' && (
          <div className="overflow-hidden rounded-xl border border-scheduled/45 bg-linear-to-br from-surface/95 via-surface/86 to-secondary/30 shadow-lg ring-1 ring-border/50">
            <div className="border-b border-border/55 bg-surface/75 px-4 py-2">
              <span className="text-sm font-semibold text-foreground">Execution Command</span>
            </div>
            <div className="p-4">
              <pre className="overflow-auto text-sm">
                <code className="font-mono text-foreground">{transformation.content}</code>
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
  head: () => ({
    meta: [
      { title: `Model Detail | ${import.meta.env.VITE_BASE_TITLE}` },
      { name: 'description', content: 'Detailed view of model configuration, coverage, and dependencies' },
      { property: 'og:description', content: 'Detailed view of model configuration, coverage, and dependencies' },
      { name: 'twitter:description', content: 'Detailed view of model configuration, coverage, and dependencies' },
    ],
  }),
});
