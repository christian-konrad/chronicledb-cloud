import React, { Component } from 'react';
import { Link } from 'react-router-dom';

import { makeStyles } from '@material-ui/core/styles';
import Button from '@material-ui/core/Button';
import Container from '@material-ui/core/Container';
import MainLayout from "../layouts/mainLayout";

const useStyles = makeStyles((theme) => ({
    root: {
        display: 'flex',
    },
    content: {
        flexGrow: 1,
        padding: theme.spacing(3),
    },
}));

export default function HomePage() {
    const classes = useStyles();

    return (
        <MainLayout>
            <Container fluid>
                <h2>Welcome!</h2>
            </Container>
        </MainLayout>
    );
}

