import React, { Component } from 'react';
import ApiClient from "../../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {Button, Card, CardContent, List, ListItem, ListItemText, Typography} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import NodeList from "../nodeList";
import ErrorBoundary from "../error/errorBoundary";
import ReplicatedCounter from "./replicatedCounter";
import { Add20 } from '@carbon/icons-react';

const useStyles = theme => ({
    addCounterButton: {
        marginTop: 24,
        background: 'white',
        marginLeft: 'auto'
    },
    addButtonContainer: {
        display: 'flex'
    }
});

class ReplicatedCounters extends Component {
    counterId;

    constructor(props) {
        super(props);
        this.state = { counterIds: [] };
        this.fetchCounters = this.fetchCounters.bind(this);
        this.createCounter = this.createCounter.bind(this);
    }

    fetchCounters() {
        ApiClient.fetchReplicatedCounterIds()
            .then(counterIds => this.setState({ counterIds }));
    }

    createCounter(counterId) {
        ApiClient.createReplicatedCounter(counterId)
            .then(() => this.fetchCounters());
    }

    componentDidMount() {
        this.fetchCounters();
        this.interval = setInterval(() => {
            this.fetchCounters();
        }, 1000);
    }

    componentWillUnmount() {
        clearInterval(this.interval);
    }

    render() {
        if (this.state.counterIds == null) return <Skeleton />

        const { classes } = this.props;
        const { counterIds } = this.state;

        return (
            <>
                {counterIds.map(counterId => <ReplicatedCounter key={counterId} counterId={counterId} />)}
                <div className={classes.addButtonContainer}>
                    <Button variant="outlined" startIcon={<Add20 />} onClick={() => this.createCounter(`counter-${counterIds.length + 1}`)} className={classes.addCounterButton}>Add new counter</Button>
                </div>
            </>
        )
    }
}
export default withStyles(useStyles)(ReplicatedCounters)
