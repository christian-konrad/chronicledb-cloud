import React, { Component } from "react";
import { Link } from 'react-router-dom';
import ApiClient from "../api/apiClient";
import MainLayout from "../layouts/mainLayout";
import {Card, CardContent, Container} from "@material-ui/core";
import SystemInfo from "../components/systemInfo";
import ErrorBoundary from "../components/error/errorBoundary";

class SystemInfoPage extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <MainLayout>
                <Container maxWidth="md">
                    <h2>System Info</h2>
                    <Card variant="outlined">
                        <CardContent>
                            <ErrorBoundary>
                                <SystemInfo />
                            </ErrorBoundary>
                        </CardContent>
                    </Card>
                </Container>
            </MainLayout>
        );
    }
}

export default SystemInfoPage;
