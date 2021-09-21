import React, { Component } from "react";
import { Link, useParams } from 'react-router-dom';
import MainLayout from "../layouts/mainLayout";
import {Breadcrumbs, Card, CardContent, Container, Typography} from "@material-ui/core";
import SystemInfo from "../components/systemInfo";
import ErrorBoundary from "../components/error/errorBoundary";
import NodeInfo from "../components/nodeInfo";

export default function NodeInfoPage() {

    const { nodeId } = useParams();

    return (
        <MainLayout>
            <Container maxWidth="md">
                <Breadcrumbs aria-label="breadcrumb">
                    <Link color="inherit" to="/admin/sys-info">
                        General system info
                    </Link>
                    <Typography color="textPrimary">Node info</Typography>
                </Breadcrumbs>
                <h2>Node Info - {nodeId}</h2>
                <Card variant="outlined">
                    <CardContent>
                        <ErrorBoundary>
                            <NodeInfo nodeId={nodeId} />
                        </ErrorBoundary>
                    </CardContent>
                </Card>
            </Container>
        </MainLayout>
    );
}
