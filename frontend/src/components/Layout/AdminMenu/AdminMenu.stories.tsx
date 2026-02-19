import type { Meta, StoryObj } from '@storybook/react-vite';
import { AuthContext, type AuthContextValue } from '@/contexts/AuthContext';
import { NotificationProvider } from '@/providers/NotificationProvider';
import { AdminMenu } from './AdminMenu';

function createAuthValue(overrides: Partial<AuthContextValue>): AuthContextValue {
  return {
    managementEnabled: true,
    authMethods: [],
    session: null,
    oauthError: null,
    isLoading: false,
    clearOAuthError: () => {},
    loginWithPassword: async () => false,
    loginWithGitHub: () => {},
    logout: () => {},
    ...overrides,
  };
}

const meta: Meta<typeof AdminMenu> = {
  title: 'Components/Layout/AdminMenu',
  component: AdminMenu,
  decorators: [
    Story => (
      <div className="flex justify-end bg-background p-8">
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof AdminMenu>;

export const NoAuth: Story = {
  decorators: [
    Story => (
      <NotificationProvider>
        <AuthContext.Provider value={createAuthValue({ authMethods: [], session: { authenticated: true } })}>
          <Story />
        </AuthContext.Provider>
      </NotificationProvider>
    ),
  ],
};

export const PasswordLocked: Story = {
  decorators: [
    Story => (
      <NotificationProvider>
        <AuthContext.Provider value={createAuthValue({ authMethods: ['password'], session: { authenticated: false } })}>
          <Story />
        </AuthContext.Provider>
      </NotificationProvider>
    ),
  ],
};

export const Authenticated: Story = {
  decorators: [
    Story => (
      <NotificationProvider>
        <AuthContext.Provider
          value={createAuthValue({
            authMethods: ['github'],
            session: { authenticated: true, username: 'octocat' },
          })}
        >
          <Story />
        </AuthContext.Provider>
      </NotificationProvider>
    ),
  ],
};
