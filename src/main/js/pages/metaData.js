import React, { Component } from "react";
import MainLayout from "../layouts/mainLayout";
import {Card, CardContent, Container} from "@material-ui/core";
import MetaData from "../components/metaData";
import ErrorBoundary from "../components/error/errorBoundary";
import SystemInfo from "../components/systemInfo";

class MetaDataPage extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <MainLayout>
                <Container maxWidth="md">
                    <h2>Meta Data</h2>
                    <Card variant="outlined">
                        <CardContent>
                            <ErrorBoundary>
                                <MetaData />
                            </ErrorBoundary>
                        </CardContent>
                    </Card>
                </Container>
            </MainLayout>
        );
    }
}

export default MetaDataPage;
