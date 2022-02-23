import React from "react";
import ReactDOM from "react-dom";
import "core-js/stable";
import "regenerator-runtime/runtime";
import 'normalize.css';
import "@fontsource/ibm-plex-sans";
import SystemInfoPage from "./pages/systemInfo";
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import { SnackbarProvider } from 'notistack';
import {createTheme} from "@material-ui/core";
import { ThemeProvider } from '@material-ui/core/styles';
import RaftGroupsPage from "./pages/raftGroups";
import RaftGroupDetailPage from "./pages/raftGroupDetails";
import NodeInfoPage from "./pages/nodeInfo";
import ReplicatedCounterPage from "./pages/replicatedCounter";
import ReplicatedEventStorePage from "./pages/eventStore/replicatedEventStore";
import ReplicatedEventStoreStreamPage from "./pages/eventStore/replicatedEventStoreStream";
import EmbeddedEventStorePage from "./pages/eventStore/embeddedEventStore";
import EmbeddedEventStoreStreamPage from "./pages/eventStore/embeddedEventStoreStream";
import HealthCheckNotifier from "./components/healthCheckNotifier";
import ErrorBoundary from "./components/error/errorBoundary";
import MetaDataPage from "./pages/metaData";

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
            'IBM Plex Sans',
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
        MuiCssBaseline: {
            '@global': {
                '*': {
                    'scrollbar-width': 'thin',
                },
                '*::-webkit-scrollbar': {
                    width: '11px',
                    height: '11px',
                },
                '*:hover::-webkit-scrollbar': {
                    width: '11px',
                    height: '11px',
                },
                '*::-webkit-scrollbar-track ': {
                    paddingRight: '15px',
                    borderRadius: '50px',
                    boxShadow: 'inset 0 0 10px 10px rgba(222,222,222,0.65)',
                    border: '2px solid transparent',
                },
                '*::-webkit-scrollbar-thumb': {
                    borderRadius: '50px',
                    boxShadow: 'inset 0 0 10px 10px rgba(177,177,177,0.7)',
                    border: '2px solid transparent',
                    background: 'transparent',
                },
            }
        },
    },
});

class App extends React.Component {

    constructor(props) {
        super(props);
        this.state = { clusterHealth: null };
        this.handleOnHealthInfoReceived = this.handleOnHealthInfoReceived.bind(this);
    }

    handleOnHealthInfoReceived(clusterHealth) {
        this.setState({ clusterHealth });
    }

    render() {
        return (
            <ThemeProvider theme={theme}>
                <SnackbarProvider maxSnack={3} anchorOrigin={{
                    vertical: 'bottom',
                    horizontal: 'right',
                }}>
                    <Router>
                        <Switch>
                            <Route path='/admin' exact={true} component={SystemInfoPage}/>
                            <Route path='/admin/sys-info' exact={true} component={SystemInfoPage}/>
                            <Route path='/admin/raft-groups' exact={true} component={RaftGroupsPage}/>
                            <Route path='/admin/raft-groups/:raftGroupId'>
                                <RaftGroupDetailPage clusterHealth={this.state.clusterHealth} />
                            </Route>
                            <Route path='/admin/meta-data' exact={true} component={MetaDataPage}/>
                            <Route path='/admin/sys-info/nodes/:nodeId' component={NodeInfoPage}/>
                            {/* TODO not admin paths, is actual app */}
                            <Route path='/admin/replicated-counter' exact={true} component={ReplicatedCounterPage}/>
                            {/*<Route path='/admin/replicated-kv-store' exact={true} component={ReplicatedKeyValueStorePage}/>*/}
                            <Route path='/admin/replicated-event-store' exact={true} component={ReplicatedEventStorePage}/>
                            <Route path='/admin/replicated-event-store/streams/:eventStreamName' component={ReplicatedEventStoreStreamPage}/>
                            <Route path='/admin/embedded-event-store' exact={true} component={EmbeddedEventStorePage}/>
                            <Route path='/admin/embedded-event-store/streams/:eventStreamName' component={EmbeddedEventStoreStreamPage}/>
                        </Switch>
                    </Router>
                    <ErrorBoundary>
                        <HealthCheckNotifier onHealthInfoReceived={this.handleOnHealthInfoReceived} />
                    </ErrorBoundary>
                </SnackbarProvider>
            </ThemeProvider>
        )
    }
}

ReactDOM.render(
    <App />,
    document.getElementById('react')
)

export default App;
