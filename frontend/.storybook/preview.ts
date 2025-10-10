import type { Preview } from '@storybook/react-vite';
import { INITIAL_VIEWPORTS } from 'storybook/viewport';
import '../src/index.css';

const preview: Preview = {
  parameters: {
    viewport: {
      viewports: INITIAL_VIEWPORTS,
    },
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },
    options: {
      storySort: {
        order: [
          'Pages',
          ['Dashboard', 'DAG', 'ModelDetail'],
          'Components',
          [
            'BackToDashboardButton',
            'ModelHeader',
            'ModelInfoCard',
            'ScheduledModelCard',
            'CoverageBar',
            'ModelCoverageRow',
            'ZoomControls',
            'IntervalTypeSection',
            'DagNode',
            'DagGraph',
            'DependencyRow',
          ],
          '*',
        ],
      },
    },
  },
};

export default preview;
