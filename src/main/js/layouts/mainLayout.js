import React, { Component } from 'react';

import { MainAppBar, MainDrawer } from '../components';
import { makeStyles } from '@material-ui/core/styles';

import CssBaseline from '@material-ui/core/CssBaseline';
import Toolbar from "@material-ui/core/Toolbar";

const useStyles = makeStyles((theme) => ({
    root: {
        display: 'flex',
    },
    content: {
        flexGrow: 1,
        padding: theme.spacing(3),
    },
}));

export default function MainLayout(props) {
    const classes = useStyles();

    const { children } = props;

    return (
        <div className={classes.root}>
            <CssBaseline />
            <MainAppBar />
            <MainDrawer />
            <main className={classes.content}>
                <Toolbar />
                {children}
            </main>
        </div>
    );
}

