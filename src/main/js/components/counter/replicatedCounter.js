import React, { Component } from 'react';
import ApiClient from "../../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {Button, Card, CardContent, List, ListItem, ListItemText, Typography} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import NodeList from "../nodeList";
import ErrorBoundary from "../error/errorBoundary";
import ReplicatedCounters from "./replicatedCounters";

const useStyles = theme => ({
    cardContent: {
        paddingBottom: 16,
        "&:last-child": {
            paddingBottom: 16
        }
    },
    incrementBox: {
        display: 'flex'
    },
    incrementButton: {
        marginLeft: 24
    },
    counterDisplay: {
        lineHeight: '34px',
        fontWeight: 600
    },
    counterId: {
        lineHeight: '34px',
        marginRight: 'auto'
    }
});

class ReplicatedCounter extends Component {
    counterId;

    constructor(props) {
        super(props);
        this.counterId = props.counterId;
        this.state = { replicatedCounter: null };
        this.updateCounter = this.updateCounter.bind(this);
        this.increment = this.increment.bind(this);
    }

    updateCounter() {
        ApiClient.fetchReplicatedCounter(this.counterId)
            .then(replicatedCounter => this.setState({ replicatedCounter }))
            .catch(error => this.setState({ replicatedCounter: null }));
    }

    componentDidMount() {
        this.updateCounter();
        // this.interval = setInterval(() => {
        //     this.updateCounter();
        // }, 1000);
    }

    increment() {
        ApiClient.incrementReplicatedCounter(this.counterId)
            .then(() => this.updateCounter());
    }

    render() {
        if (this.state.replicatedCounter == null) return <Skeleton />

        const { classes } = this.props;
        const replicatedCounter = this.state.replicatedCounter;

        return (
            <Card variant="outlined">
                <CardContent className={classes.cardContent}>
                    <ErrorBoundary>
                        <div className={classes.incrementBox}>
                            <Typography className={classes.counterId}>{this.counterId}</Typography>
                            <Typography className={classes.counterDisplay}>{replicatedCounter}</Typography>
                            <Button variant="outlined" onClick={this.increment} className={classes.incrementButton}>Increment</Button>
                        </div>
                    </ErrorBoundary>
                </CardContent>
            </Card>
        )
    }
}
export default withStyles(useStyles)(ReplicatedCounter)
