import type { Meta, StoryObj } from '@storybook/react-vite';
import { http, HttpResponse } from 'msw';
import { AuthContext, type AuthContextValue } from '@/contexts/AuthContext';
import { NotificationProvider } from '@/providers/NotificationProvider';
import { ModelAdminActions } from './ModelAdminActions';

function createAuthValue(overrides: Partial<AuthContextValue>): AuthContextValue {
  return {
    managementEnabled: true,
    authMethods: ['password'],
    session: { authenticated: true },
    isLoading: false,
    loginWithPassword: async () => false,
    loginWithGitHub: () => {},
    logout: () => {},
    ...overrides,
  };
}

const meta: Meta<typeof ModelAdminActions> = {
  title: 'Components/Domain/Models/ModelAdminActions',
  component: ModelAdminActions,
  decorators: [
    Story => (
      <AuthContext.Provider value={createAuthValue({})}>
        <NotificationProvider>
          <div className="bg-background p-8">
            <Story />
          </div>
        </NotificationProvider>
      </AuthContext.Provider>
    ),
  ],
  parameters: {
    msw: {
      handlers: [
        http.post('/api/v1/admin/models/:id/consolidate', () =>
          HttpResponse.json({ model_id: 'db.table', ranges_merged: 5 })
        ),
        http.post('/api/v1/admin/models/:id/delete-period', () =>
          HttpResponse.json({ model_id: 'db.table', deleted_rows: 5, cascade_results: [] })
        ),
        http.put('/api/v1/admin/models/:id/bounds', () =>
          HttpResponse.json({ model_id: 'db.table', min: 100, max: 999 })
        ),
        http.delete('/api/v1/admin/models/:id/bounds', () =>
          HttpResponse.json({ model_id: 'db.table', deleted: true })
        ),
        http.post('/api/v1/admin/models/:id/refresh-bounds', () =>
          HttpResponse.json({ model_id: 'db.table', scan_type: 'full' })
        ),
        http.post('/api/v1/admin/models/:id/run-now', () =>
          HttpResponse.json({ model_id: 'db.table', status: 'enqueued' })
        ),
        http.get('/api/v1/admin/models/:id/config-override', () => new HttpResponse(null, { status: 404 })),
        http.put('/api/v1/admin/models/:id/config-override', () =>
          HttpResponse.json({ model_id: 'db.table', model_type: 'transformation' })
        ),
        http.delete('/api/v1/admin/models/:id/config-override', () => HttpResponse.json({ deleted: true })),
      ],
    },
  },
};

export default meta;
type Story = StoryObj<typeof ModelAdminActions>;

export const Default: Story = {
  args: {
    modelId: 'db.table',
    modelType: 'incremental',
  },
};

export const WithTransformations: Story = {
  args: {
    modelId: 'db.table',
    modelType: 'incremental',
    transformations: [
      { name: 'Slot', expression: 'math.floor((value - 1606824023) / 12)' },
      { name: 'Datetime', expression: 'value * 1000', format: 'datetime' },
      { name: 'Timestamp' },
    ],
  },
};

export const ErrorResponse: Story = {
  args: {
    modelId: 'db.table',
    modelType: 'incremental',
  },
  parameters: {
    msw: {
      handlers: [
        http.post('/api/v1/admin/models/:id/consolidate', () =>
          HttpResponse.json({ error: 'Nothing to consolidate' }, { status: 400 })
        ),
      ],
    },
  },
};

export const External: Story = {
  args: {
    modelId: 'db.ext_table',
    modelType: 'external',
    currentMin: 100000,
    currentMax: 999999,
  },
};

export const ExternalNoBounds: Story = {
  args: {
    modelId: 'db.ext_table',
    modelType: 'external',
  },
};

export const WithCoverageRanges: Story = {
  args: {
    modelId: 'db.table',
    modelType: 'incremental',
    transformations: [
      { name: 'Slot', expression: 'math.floor((value - 1606824023) / 12)' },
      { name: 'Datetime', expression: 'value * 1000', format: 'datetime' },
      { name: 'Timestamp' },
    ],
    coverageRanges: [
      { position: 1606824023, interval: 2592000 },
      { position: 1609416023, interval: 2592000 },
      { position: 1614600023, interval: 5184000 },
    ],
  },
};

export const WithActiveOverride: Story = {
  args: {
    modelId: 'db.table',
    modelType: 'incremental',
    currentOverride: {
      model_id: 'db.table',
      model_type: 'transformation',
      enabled: true,
      override: { interval: { max: 100 } },
      updated_at: new Date().toISOString(),
    },
  },
};

export const ModelDisabled: Story = {
  args: {
    modelId: 'db.table',
    modelType: 'incremental',
    currentOverride: {
      model_id: 'db.table',
      model_type: 'transformation',
      enabled: false,
      override: null,
      updated_at: new Date().toISOString(),
    },
  },
};

export const Scheduled: Story = {
  args: {
    modelId: 'db.scheduled_table',
    modelType: 'scheduled',
  },
};

export const ScheduledWithOverride: Story = {
  args: {
    modelId: 'db.scheduled_table',
    modelType: 'scheduled',
    currentOverride: {
      model_id: 'db.scheduled_table',
      model_type: 'transformation',
      enabled: true,
      override: { schedule: '@every 30s' },
      updated_at: new Date().toISOString(),
    },
  },
};

export const ScheduledDisabled: Story = {
  args: {
    modelId: 'db.scheduled_table',
    modelType: 'scheduled',
    currentOverride: {
      model_id: 'db.scheduled_table',
      model_type: 'transformation',
      enabled: false,
      override: null,
      updated_at: new Date().toISOString(),
    },
  },
};

export const ScheduledRunNowAlreadyRunning: Story = {
  args: {
    modelId: 'db.scheduled_table',
    modelType: 'scheduled',
  },
  parameters: {
    msw: {
      handlers: [
        http.post('/api/v1/admin/models/:id/run-now', () =>
          HttpResponse.json({ error: 'Scheduled run already in progress' }, { status: 409 })
        ),
      ],
    },
  },
};

export const ExternalWithOverride: Story = {
  args: {
    modelId: 'db.ext_table',
    modelType: 'external',
    currentMin: 100000,
    currentMax: 999999,
    currentOverride: {
      model_id: 'db.ext_table',
      model_type: 'external',
      override: { lag: 100 },
      updated_at: new Date().toISOString(),
    },
  },
};
