import React, { Component } from "react";
import MainLayout from "../../layouts/mainLayout";
import {Container} from "@material-ui/core";
import ErrorBoundary from "../../components/error/errorBoundary";
import {withStyles} from '@material-ui/core/styles';
import EventStoreStreams from "../../components/eventStore/eventStoreStreams";

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
                        <EventStoreStreams type="replicated" />
                    </ErrorBoundary>
                </Container>
            </MainLayout>
        );
    }
}

export default withStyles(useStyles)(ReplicatedEventStorePage);
