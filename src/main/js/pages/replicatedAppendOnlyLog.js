import React, { Component } from "react";
import { Link } from 'react-router-dom';
import ApiClient from "../api/apiClient";
import MainLayout from "../layouts/mainLayout";
import {Card, CardContent, Container} from "@material-ui/core";
import SystemInfo from "../components/systemInfo";
import ErrorBoundary from "../components/error/errorBoundary";
import ReplicatedCounter from "../components/replicatedCounter";
import {withStyles} from '@material-ui/core/styles';
import ReplicatedCounters from "../components/replicatedCounters";
import WorkInProgressPlaceholder from "../components/workInProgressPlaceholder";

const useStyles = theme => ({
    cardContent: {
        paddingBottom: 16,
        "&:last-child": {
            paddingBottom: 16
        }
    },
});

class ReplicatedAppendOnlyLogPage extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        const { classes } = this.props;

        return (
            <MainLayout>
                <Container maxWidth="md">
                    <h2>Replicated Append-Only Log</h2>
                    <ErrorBoundary>
                        <WorkInProgressPlaceholder />
                    </ErrorBoundary>
                </Container>
            </MainLayout>
        );
    }
}

export default withStyles(useStyles)(ReplicatedAppendOnlyLogPage);
