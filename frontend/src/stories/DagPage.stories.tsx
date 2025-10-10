import type { Meta, StoryObj } from '@storybook/react-vite';
import { DagPage } from './DagPage';

const meta = {
  title: 'Pages/DAG',
  component: DagPage,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
} satisfies Meta<typeof DagPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    showLinks: false,
  },
};
