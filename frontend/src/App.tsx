import { type JSX } from 'react';
import Logo from '/logo.png';
import { ModelsList } from './components/ModelsList';

function App(): JSX.Element {
  return (
    <>
      <img src={Logo} className="logo" alt="Vite logo" />
      <ModelsList />
    </>
  );
}

export default App;
