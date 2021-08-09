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
                        <Route path='/admin' exact={true} component={HomePage}/>
                        <Route path='/admin/sys-info' exact={true} component={SystemInfoPage}/>
                        <Route path='/admin/raft-groups' exact={true} component={RaftGroupsPage}/>
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
