import React, { Component } from "react";
import { Link } from 'react-router-dom';
import ApiClient from "../api/apiClient";
import MainLayout from "../layouts/mainLayout";
import {Card, CardContent, Container} from "@material-ui/core";
import SystemInfo from "../components/systemInfo";
import ErrorBoundary from "../components/error/errorBoundary";
import RaftGroups from "../components/raftGroups";

class RaftGroupsPage extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <MainLayout>
                <Container maxWidth="md">
                    <h2>Raft Groups</h2>
                    <ErrorBoundary>
                        <RaftGroups />
                    </ErrorBoundary>
                </Container>
            </MainLayout>
        );
    }
}

export default RaftGroupsPage;
