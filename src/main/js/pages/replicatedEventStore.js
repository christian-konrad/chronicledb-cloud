import React, { Component } from "react";
import { Link } from 'react-router-dom';
import ApiClient from "../api/apiClient";
import MainLayout from "../layouts/mainLayout";
import {Card, CardContent, Container} from "@material-ui/core";
import ErrorBoundary from "../components/error/errorBoundary";
import {withStyles} from '@material-ui/core/styles';
import ReplicatedEventStoreStreams from "../components/eventStore/replicatedEventStoreStreams";

const useStyles = theme => ({
    cardContent: {
        paddingBottom: 16,
        "&:last-child": {
            paddingBottom: 16
        }
    },
});

class ReplicatedEventStorePage extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        const { classes } = this.props;

        return (
            <MainLayout>
                <Container maxWidth="md">
                    <h2>Replicated ChronicleDB Event Store</h2>
                    <ErrorBoundary>
                        <ReplicatedEventStoreStreams />
                    </ErrorBoundary>
                </Container>
            </MainLayout>
        );
    }
}

export default withStyles(useStyles)(ReplicatedEventStorePage);
