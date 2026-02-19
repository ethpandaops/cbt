import type { Meta, StoryObj } from '@storybook/react-vite';
import { useState, type JSX } from 'react';
import { Notification } from './Notification';

const meta = {
  title: 'Components/Overlays/Notification',
  component: Notification,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  argTypes: {
    open: { control: 'boolean' },
    position: {
      control: 'select',
      options: ['top-left', 'top-center', 'top-right', 'bottom-left', 'bottom-center', 'bottom-right'],
    },
    variant: {
      control: 'select',
      options: ['info', 'success', 'warning', 'danger'],
    },
    duration: { control: 'number' },
    showCloseButton: { control: 'boolean' },
  },
  decorators: [
    Story => (
      <div className="flex min-h-dvh items-center justify-center bg-background p-6">
        <Story />
      </div>
    ),
  ],
} satisfies Meta;

export default meta;
type Story = StoryObj;

export const Interactive: Story = {
  render: function InteractiveDemo(): JSX.Element {
    const [show, setShow] = useState(false);

    return (
      <>
        <button
          type="button"
          onClick={() => setShow(true)}
          className="rounded-sm bg-accent px-3 py-1.5 text-sm font-medium text-white"
        >
          Show Notification
        </button>
        <Notification
          open={show}
          onClose={() => setShow(false)}
          variant="success"
          position="top-center"
          duration={3000}
        >
          Action completed successfully!
        </Notification>
      </>
    );
  },
};

export const Success: Story = {
  render: function SuccessDemo(): JSX.Element {
    const [show, setShow] = useState(false);

    return (
      <>
        <button
          type="button"
          onClick={() => setShow(true)}
          className="rounded-sm bg-success px-3 py-1.5 text-sm font-medium text-white"
        >
          Trigger Success
        </button>
        <Notification
          open={show}
          onClose={() => setShow(false)}
          variant="success"
          position="top-center"
          duration={3000}
        >
          Copied to clipboard!
        </Notification>
      </>
    );
  },
};

export const Danger: Story = {
  render: function DangerDemo(): JSX.Element {
    const [show, setShow] = useState(false);

    return (
      <>
        <button
          type="button"
          onClick={() => setShow(true)}
          className="rounded-sm bg-danger px-3 py-1.5 text-sm font-medium text-white"
        >
          Trigger Error
        </button>
        <Notification open={show} onClose={() => setShow(false)} variant="danger" position="top-center" duration={3000}>
          Failed to save changes
        </Notification>
      </>
    );
  },
};

export const WithCloseButton: Story = {
  render: function WithCloseButtonDemo(): JSX.Element {
    const [show, setShow] = useState(false);

    return (
      <>
        <button
          type="button"
          onClick={() => setShow(true)}
          className="rounded-sm bg-accent px-3 py-1.5 text-sm font-medium text-white"
        >
          Show Persistent
        </button>
        <Notification
          open={show}
          onClose={() => setShow(false)}
          variant="info"
          position="top-center"
          duration={0}
          showCloseButton
        >
          This notification stays until you close it
        </Notification>
      </>
    );
  },
};
