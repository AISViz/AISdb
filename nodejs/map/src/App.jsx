import { useState } from 'react'
import logo from './logo.svg'
import './App.css'

function App() {
  const [count, setCount] = useState(0)

  return  (
    <div id="root">
      <div id="map" class="map"></div>
      <div id="optionSelectContainer" class="optionSelectContainer">

        <label for="startSelect">Start</label>
        <input type="date" id="startSelect" name="start"></input>

        <label for="endSelect">End</label>
        <input type="date" id="endSelect" name="end"></input>

      </div>
    </div>
    /*
    <div id='test'></div>
    */
  )
}
    //<script type="module" src="./src/main.jsx"></script>
    //<script type="module" src="./map.js" ></script>

export default App
