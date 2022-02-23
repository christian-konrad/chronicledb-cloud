import React, { Component } from "react";
import {Link, useParams} from 'react-router-dom';
import MainLayout from "../../layouts/mainLayout";
import {Breadcrumbs, Container, Typography} from "@material-ui/core";
import ErrorBoundary from "../../components/error/errorBoundary";
import EventStoreStream from "../../components/eventStore/eventStoreStream";

export default function EmbeddedEventStoreStreamPage() {

    const { eventStreamName } = useParams();

    return (
        <MainLayout>
            <Container maxWidth="md">
                <Breadcrumbs aria-label="breadcrumb">
                    <Link color="inherit" to="/admin/embedded-event-store">
                        Embedded Event Store
                    </Link>
                    <Typography color="textPrimary">Event Store</Typography>
                </Breadcrumbs>
                <h2>{eventStreamName}</h2>
                <ErrorBoundary>
                    <EventStoreStream key={eventStreamName} eventStreamName={eventStreamName} type='embedded' />
                </ErrorBoundary>
            </Container>
        </MainLayout>
    );
}
