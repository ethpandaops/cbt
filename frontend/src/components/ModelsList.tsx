import { type JSX } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getModelsOptions } from '@api/@tanstack/react-query.gen';

export function ModelsList(): JSX.Element {
  const { data, error, isLoading } = useQuery(
    getModelsOptions({
      query: {
        type: 'transformation',
      },
    })
  );

  if (isLoading) return <div>Loading models...</div>;
  if (error) {
    return <div>Error: {error.message}</div>;
  }

  return (
    <div>
      <h2>Models ({data?.total})</h2>
      <ul>
        {data?.models.map(model => (
          <li key={model.id}>
            <strong>{model.id}</strong>
            <div>Type: {model.type}</div>
            <div>
              Database: {model.database}, Table: {model.table}
            </div>
            {model.dependencies && model.dependencies.length > 0 && (
              <div>Dependencies: {model.dependencies.join(', ')}</div>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
