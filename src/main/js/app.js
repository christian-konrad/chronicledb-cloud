import React from "react";
import ReactDOM from "react-dom";
import "core-js/stable";
import "regenerator-runtime/runtime";
import 'normalize.css';
import "./css/app.css";
import HomePage from "./pages/home";
import SystemInfoPage from "./pages/systemInfo";
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import {createTheme} from "@material-ui/core";
import { ThemeProvider } from '@material-ui/core/styles';
import RaftGroupsPage from "./pages/raftGroups";
import NodeInfoPage from "./pages/nodeInfo";
import ReplicatedCounterPage from "./pages/replicatedCounter";
import ReplicatedEventStorePage from "./pages/replicatedEventStore";
// import ReplicatedKeyValueStorePage from "./pages/replicatedKeyValueStore";
// import ReplicatedAppendOnlyLogPage from "./pages/replicatedAppendOnlyLog";

const theme = createTheme({
    props: {
        MuiListItem: {
            dense: true,
        },
        MuiTextField: {
            margin: 'dense',
        },
    },
    typography: {
        fontFamily: [
            'Inter',
            '-apple-system',
            'BlinkMacSystemFont',
            '"Segoe UI"',
            'Roboto',
            '"Helvetica Neue"',
            'Arial',
            'sans-serif',
            '"Apple Color Emoji"',
            '"Segoe UI Emoji"',
            '"Segoe UI Symbol"',
        ].join(','),
        fontSize: 13,
        body2: {
            fontWeight: 600,
            fontSize: 14,
        }
    },
    palette: {
        primary: {
            main: '#1e212a',
        },
        secondary: {
            main: '#5046e4',
            // dark: will be calculated from palette.secondary.main,
            contrastText: '#ffffff',
        },
        // Used by `getContrastText()` to maximize the contrast between
        // the background and the text.
        contrastThreshold: 3,
        // Used by the functions below to shift a color's luminance by approximately
        // two indexes within its tonal palette.
        // E.g., shift from Red 500 to Red 300 or Red 700.
        tonalOffset: 0.2,
    },
    overrides: {
        // Style sheet name ⚛️
        MuiListItemIcon: {
            root: {
                minWidth: 38,
                marginLeft: 4,
            },
        },
    },
});

class App extends React.Component {
    render() {
        return (
            <ThemeProvider theme={theme}>
                <Router>
                    <Switch>
                        <Route path='/admin' exact={true} component={SystemInfoPage}/>
                        <Route path='/admin/sys-info' exact={true} component={SystemInfoPage}/>
                        <Route path='/admin/raft-groups' exact={true} component={RaftGroupsPage}/>
                        <Route path='/admin/sys-info/nodes/:nodeId' component={NodeInfoPage}/>
                        {/* TODO not admin paths, is actual app */}
                        <Route path='/admin/replicated-counter' exact={true} component={ReplicatedCounterPage}/>
                        {/*<Route path='/admin/replicated-kv-store' exact={true} component={ReplicatedKeyValueStorePage}/>*/}
                        <Route path='/admin/replicated-event-store' exact={true} component={ReplicatedEventStorePage}/>
                    </Switch>
                </Router>
            </ThemeProvider>
        )
    }
}

ReactDOM.render(
    <App />,
    document.getElementById('react')
)

export default App;
