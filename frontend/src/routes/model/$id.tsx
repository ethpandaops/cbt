import { type JSX, useCallback } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  getExternalModelOptions,
  getExternalBoundsOptions,
  listExternalBoundsOptions,
  getTransformationOptions,
  listTransformationCoverageOptions,
  listTransformationsOptions,
  getIntervalTypesOptions,
} from '@/api/@tanstack/react-query.gen';
import { BackToDashboardButton } from '@/components/Elements/BackToDashboardButton';
import { ErrorState } from '@/components/Feedback/ErrorState';
import { ModelHeader } from '@/components/Domain/Models/ModelHeader';
import { ModelInfoCard, type InfoField } from '@/components/Domain/Models/ModelInfoCard';
import { ModelDetailView } from '@/components/Domain/Models/ModelDetailView';
import { ModelSkeleton } from '@/components/Domain/Models/ModelSkeleton';
import { ModelAdminActions } from '@/components/Domain/Models/ModelAdminActions';
import { SQLCodeBlock } from '@/components/Elements/SQLCodeBlock';
import { adminFetch } from '@/utils/admin-api';
import { getErrorCode } from '@/utils/error';
import { timeAgo } from '@/utils/time';

interface ConfigOverride {
  model_id: string;
  model_type: string;
  enabled?: boolean;
  override: Record<string, unknown> | null;
  updated_at: string;
}

interface ConfigOverrideResponse {
  base_config?: Record<string, unknown>;
  model_id?: string;
  model_type?: string;
  enabled?: boolean;
  override?: Record<string, unknown> | null;
  updated_at?: string;
}

function useConfigOverride(modelId: string): {
  override: ConfigOverride | null;
  baseConfig: Record<string, unknown> | null;
  invalidate: () => void;
} {
  const encodedId = encodeURIComponent(modelId);
  const queryClient = useQueryClient();

  const query = useQuery<ConfigOverrideResponse | null>({
    queryKey: ['config-override', modelId],
    queryFn: async () => {
      const resp = await adminFetch(`/api/v1/models/${encodedId}/config-override`);
      if (!resp.ok) return null;
      return resp.json();
    },
    enabled: !!modelId,
    staleTime: 10_000,
  });

  const invalidate = useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: ['config-override', modelId] });
    // Also invalidate model data queries so the UI reflects overridden values
    void queryClient.invalidateQueries({
      queryKey: getExternalModelOptions({ path: { id: modelId } }).queryKey,
    });
    void queryClient.invalidateQueries({
      queryKey: getTransformationOptions({ path: { id: modelId } }).queryKey,
    });
  }, [queryClient, modelId]);

  const data = query.data;
  const baseConfig = data?.base_config ?? null;

  // Build ConfigOverride only when the response contains override fields
  const override: ConfigOverride | null =
    data?.model_id != null
      ? {
          model_id: data.model_id,
          model_type: data.model_type!,
          enabled: data.enabled,
          override: data.override ?? null,
          updated_at: data.updated_at!,
        }
      : null;

  return { override, baseConfig, invalidate };
}

/** Resolve a nested value from the override object. Returns undefined if not set. */
function getOverrideValue(override: Record<string, unknown> | null | undefined, ...path: string[]): unknown {
  let current: unknown = override;

  for (const key of path) {
    if (current == null || typeof current !== 'object') return undefined;
    current = (current as Record<string, unknown>)[key];
  }

  return current;
}

function ModelDetailComponent(): JSX.Element {
  const { id } = Route.useParams();
  const decodedId = decodeURIComponent(id);

  // Load all models to determine type
  const allModels = useQuery(listAllModelsOptions());
  const model = allModels.data?.models.find(m => m.id === decodedId);

  // Fetch interval types for transformation selector
  const intervalTypes = useQuery(getIntervalTypesOptions());

  // Fetch config override data (public read)
  const { override: currentOverride, baseConfig, invalidate: invalidateOverride } = useConfigOverride(decodedId);

  // Conditional queries based on model type
  const externalModel = useQuery({
    ...getExternalModelOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'external',
  });

  const externalBounds = useQuery({
    ...getExternalBoundsOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'external',
    throwOnError: error => getErrorCode(error) !== 404,
    retry: (failureCount, error) => {
      if (getErrorCode(error) === 404) return false;
      return failureCount < 3;
    },
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
    if (externalModel.isLoading) {
      return <ModelSkeleton />;
    }

    const bounds = externalBounds.data;
    const extModel = externalModel.data;
    const isBoundsLoading = externalBounds.isLoading;

    const boundsLoadingPlaceholder = (
      <span
        className="inline-block h-5 w-16 animate-shimmer rounded-sm bg-linear-to-r from-secondary/80 via-surface to-accent/20 bg-[length:200%_100%]"
        aria-label="Loading bounds"
      />
    );

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

    fields.push(
      {
        label: 'Min Position',
        value: isBoundsLoading ? boundsLoadingPlaceholder : bounds ? bounds.min.toLocaleString() : 'N/A',
        variant: 'highlight',
        highlightColor: 'external',
      },
      {
        label: 'Max Position',
        value: isBoundsLoading ? boundsLoadingPlaceholder : bounds ? bounds.max.toLocaleString() : 'N/A',
        variant: 'highlight',
        highlightColor: 'external',
      }
    );

    return (
      <div className="space-y-6">
        <ModelHeader modelId={decodedId} modelType="external" />
        <BackToDashboardButton />
        <ModelAdminActions
          modelId={decodedId}
          modelType="external"
          currentMin={bounds?.min}
          currentMax={bounds?.max}
          onBoundsChanged={() => void externalBounds.refetch()}
          coverageRanges={coverage.data?.coverage?.find(c => c.id === decodedId)?.ranges}
          currentOverride={currentOverride}
          baseConfig={baseConfig}
          onOverrideChanged={invalidateOverride}
        />
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

  const ov = currentOverride?.override;

  if (!isIncremental) {
    // Scheduled transformation - no coverage
    const scheduleOverride = getOverrideValue(ov, 'schedule') as string | undefined;
    const tagsOverride = getOverrideValue(ov, 'tags') as string[] | undefined;

    // Use baseConfig for accurate comparison (base_config comes from pre-override snapshot)
    const baseSchedule = (baseConfig?.schedule as string | undefined) ?? transformation?.schedule;
    const baseTags = (baseConfig?.tags as string[] | undefined) ?? transformation?.tags ?? [];

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

    {
      const effectiveSchedule = scheduleOverride ?? baseSchedule;

      if (effectiveSchedule) {
        fields.push({
          label: 'Schedule',
          value: effectiveSchedule,
          variant: 'highlight',
          highlightColor: 'scheduled',
          ...(scheduleOverride != null &&
            scheduleOverride !== baseSchedule && { overridden: true, originalValue: baseSchedule }),
        });
      }
    }

    if (transformation?.metadata?.last_run_at) {
      fields.push({ label: 'Last Run', value: timeAgo(transformation.metadata.last_run_at) });
    }

    if (transformation?.metadata?.last_run_status) {
      fields.push({ label: 'Status', value: transformation.metadata.last_run_status });
    }

    {
      const effectiveTags = tagsOverride ?? baseTags;

      if (effectiveTags.length > 0 || tagsOverride != null) {
        const tagsChanged = tagsOverride != null && JSON.stringify(tagsOverride) !== JSON.stringify(baseTags);
        fields.push({
          label: 'Tags',
          value: effectiveTags.join(', ') || 'none',
          ...(tagsChanged && { overridden: true, originalValue: baseTags.join(', ') || 'none' }),
        });
      }
    }

    if (transformation?.depends_on && transformation.depends_on.length > 0) {
      fields.push({ label: 'Dependencies', value: `${transformation.depends_on.length} model(s)` });
    }

    return (
      <div className="space-y-6">
        <ModelHeader modelId={decodedId} modelType="scheduled" />
        <BackToDashboardButton />
        <ModelAdminActions
          modelId={decodedId}
          modelType="scheduled"
          currentOverride={currentOverride}
          baseConfig={baseConfig}
          onOverrideChanged={invalidateOverride}
        />
        <ModelInfoCard title="Transformation Details" fields={fields} borderColor="border-scheduled/30" />

        {transformation?.content && transformation.content_type === 'sql' && (
          <SQLCodeBlock sql={transformation.content} title="Transformation Query" />
        )}

        {transformation?.content && transformation.content_type === 'exec' && (
          <div className="glass-surface overflow-hidden border-scheduled/35">
            <div className="border-b border-border/50 bg-surface/78 px-4 py-2">
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
    <div className="space-y-6">
      <ModelHeader modelId={decodedId} modelType="incremental" />
      <BackToDashboardButton />
      <ModelAdminActions
        modelId={decodedId}
        modelType="incremental"
        transformations={intervalTypes.data?.interval_types?.[transformation?.interval?.type || ''] || []}
        coverageRanges={coverage.data?.coverage?.find(c => c.id === decodedId)?.ranges}
        currentOverride={currentOverride}
        baseConfig={baseConfig}
        onOverrideChanged={invalidateOverride}
      />
      <ModelDetailView
        decodedId={decodedId}
        transformation={transformation}
        coverage={coverage}
        allBounds={allBounds}
        allTransformations={allTransformations}
        intervalTypes={intervalTypes}
        currentOverride={currentOverride}
        baseConfig={baseConfig}
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
