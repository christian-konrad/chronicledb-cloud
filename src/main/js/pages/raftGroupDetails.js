import React, { Component } from "react";
import { Link, useParams } from 'react-router-dom';
import MainLayout from "../layouts/mainLayout";
import {Breadcrumbs, Card, CardContent, Container, Typography} from "@material-ui/core";
import ErrorBoundary from "../components/error/errorBoundary";
import RaftGroupDetails from "../components/raftGroupDetails";

export default function RaftGroupDetailPage({ clusterHealth }) {

    const { raftGroupId } = useParams();

    return (
        <MainLayout>
            <Container maxWidth="md">
                <Breadcrumbs aria-label="breadcrumb">
                    <Link color="inherit" to="/admin/raft-groups">
                        Raft groups
                    </Link>
                    <Typography color="textPrimary">Raft group details</Typography>
                </Breadcrumbs>
                <h2>Raft Group Details - {raftGroupId}</h2>
                <Card variant="outlined">
                    <CardContent>
                        <ErrorBoundary>
                            <RaftGroupDetails raftGroupId={raftGroupId} clusterHealth={clusterHealth} />
                        </ErrorBoundary>
                    </CardContent>
                </Card>
            </Container>
        </MainLayout>
    );
}
