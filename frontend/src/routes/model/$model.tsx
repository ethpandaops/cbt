import { type JSX } from 'react';
import { createFileRoute } from '@tanstack/react-router';

function ModelComponent(): JSX.Element {
  const { model } = Route.useParams();

  return (
    <div className="space-y-6">
      <div className="rounded-lg bg-white p-6 shadow-sm">
        <h2 className="text-2xl font-bold text-gray-900">Model: {model}</h2>
        <p className="mt-2 text-gray-600">
          This is the detail page for the model: <span className="font-mono font-semibold">{model}</span>
        </p>
      </div>

      <div className="rounded-lg bg-white p-6 shadow-sm">
        <h3 className="text-lg font-semibold text-gray-900">Model Information</h3>
        <div className="mt-4 space-y-2">
          <div className="flex justify-between border-b border-gray-200 py-2">
            <span className="text-gray-600">Model ID:</span>
            <span className="font-mono font-medium text-gray-900">{model}</span>
          </div>
          <div className="flex justify-between border-b border-gray-200 py-2">
            <span className="text-gray-600">Status:</span>
            <span className="rounded-full bg-green-100 px-3 py-1 text-sm font-medium text-green-800">Active</span>
          </div>
        </div>
      </div>

      <div className="rounded-lg bg-blue-50 p-6">
        <h3 className="text-lg font-semibold text-blue-900">Actions</h3>
        <div className="mt-4 flex gap-3">
          <button
            type="button"
            className="rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
          >
            View Details
          </button>
          <button
            type="button"
            className="rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
          >
            Edit Model
          </button>
        </div>
      </div>
    </div>
  );
}

export const Route = createFileRoute('/model/$model')({
  component: ModelComponent,
});
