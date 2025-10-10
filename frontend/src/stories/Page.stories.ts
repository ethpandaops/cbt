import type { Meta, StoryObj } from '@storybook/react-vite';
import { Page } from './Page';

const meta = {
  title: 'Pages/ModelDetail',
  component: Page,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
} satisfies Meta<typeof Page>;

export default meta;
type Story = StoryObj<typeof meta>;

export const ExternalModel: Story = {
  args: {
    modelType: 'external',
  },
};

export const ScheduledModel: Story = {
  args: {
    modelType: 'scheduled',
  },
};

export const IncrementalModel: Story = {
  args: {
    modelType: 'incremental',
  },
};
