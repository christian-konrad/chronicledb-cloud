import React, {Component} from 'react';
import {AppBar, Toolbar, IconButton, Typography} from '@material-ui/core';
import MenuIcon from '@material-ui/icons/Menu';
import { withStyles  } from '@material-ui/core/styles';
import {Link} from 'react-router-dom';

const useStyles = theme => ({
    appBar: {
        zIndex: theme.zIndex.drawer + 1,
    },
    menuButton: {
        marginRight: theme.spacing(2),
    },
    title: {
        flexGrow: 1,
    },
});

class MainAppBar extends Component {
    constructor(props) {
        super(props);
        this.state = { isOpen: false };
        this.toggle = this.toggle.bind(this);
    }

    toggle() {
        this.setState({
            isOpen: !this.state.isOpen
        });
    }

    render() {
        const { classes } = this.props;

        return <AppBar position="fixed" className={classes.appBar}>
            <Toolbar>
                <Typography variant="h6" className={classes.title} tag={Link} to="/admin">
                    Raft Log Replication Demo
                </Typography>
            </Toolbar>
        </AppBar>;
    }
}

export default withStyles(useStyles)(MainAppBar)
