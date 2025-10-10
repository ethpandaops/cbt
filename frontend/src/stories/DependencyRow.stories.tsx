import type { Meta, StoryObj } from '@storybook/react-vite';
import { DependencyRow } from '@/components/DependencyRow';
import { Tooltip } from 'react-tooltip';

const meta = {
  title: 'Components/DependencyRow',
  component: DependencyRow,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <div className="max-w-4xl">
          <Story />
        </div>
        <Tooltip id="dep-tooltip" className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100" />
      </div>
    ),
  ],
} satisfies Meta<typeof DependencyRow>;

export default meta;
type Story = StoryObj<typeof meta>;

export const TransformationDependency: Story = {
  args: {
    dependencyId: 'beacon_api.validators',
    type: 'transformation',
    ranges: [
      { position: 100, interval: 200 },
      { position: 400, interval: 150 },
    ],
    zoomStart: 0,
    zoomEnd: 1000,
    tooltipId: 'dep-tooltip',
    showLink: false,
  },
};

export const ExternalDependency: Story = {
  args: {
    dependencyId: 'beacon_api.blocks',
    type: 'external',
    bounds: { min: 0, max: 800 },
    zoomStart: 0,
    zoomEnd: 1000,
    tooltipId: 'dep-tooltip',
    showLink: false,
  },
};

export const ScheduledDependency: Story = {
  args: {
    dependencyId: 'beacon_api.daily_summary',
    type: 'scheduled',
    zoomStart: 0,
    zoomEnd: 1000,
    tooltipId: 'dep-tooltip',
    showLink: false,
  },
};

export const LongName: Story = {
  args: {
    dependencyId: 'beacon_api.very_long_dependency_name_that_should_be_displayed_properly',
    type: 'transformation',
    ranges: [{ position: 200, interval: 300 }],
    zoomStart: 0,
    zoomEnd: 1000,
    tooltipId: 'dep-tooltip',
    showLink: false,
  },
};

export const WithTransformation: Story = {
  args: {
    dependencyId: 'beacon_api.transformed_data',
    type: 'transformation',
    ranges: [{ position: 300, interval: 400 }],
    zoomStart: 0,
    zoomEnd: 1000,
    transformation: {
      name: 'Slot Number',
      expression: 'x',
    },
    tooltipId: 'dep-tooltip',
    showLink: false,
  },
};
