import React, { Component } from 'react';
import ApiClient from "../../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {
    Accordion,
    AccordionDetails,
    AccordionSummary,
    Button,
    Typography
} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import ErrorBoundary from "../error/errorBoundary";
import ReactJson from 'react-json-view';
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";

const useStyles = theme => ({
    accordionDetails: {
        display: 'block'
    },
    muiTypography: {
        subtitle2: {
            fontWeight: 600,
        }
    },
    pushEventBox: {
        display: 'flex',
        flexDirection: 'column'
    },
    pushEventButton: {
        marginLeft: 24
    },
    streamDisplay: {
        lineHeight: '34px',
        fontWeight: 600
    },
    streamName: {
        lineHeight: '34px',
        marginRight: 'auto'
    }
});

class EventStore extends Component {
    eventStreamName;

    constructor(props) {
        super(props);
        this.eventStreamName = props.eventStreamName;
        this.state = { streamInfo: null };
        this.updateStreamInfo = this.updateStreamInfo.bind(this);
        this.pushEvent = this.pushEvent.bind(this);
    }

    updateStreamInfo() {
        ApiClient.fetchEventStreamInfo(this.eventStreamName)
            .then(streamInfo => this.setState({ streamInfo }))
            .catch(error => this.setState({ streamInfo: null }));
    }

    componentDidMount() {
        this.updateStreamInfo();
        // this.interval = setInterval(() => {
        //     this.updateCounter();
        // }, 1000);
    }

    // TODO remove mock event
    pushEvent(event) {
        ApiClient.pushEvent(this.eventStreamName, event)
            .then(() => this.updateStreamInfo());
    }

    _createMockEvent = () => ({
        "someBool": true,
        "someInt": 1,
        "someString": "foo",
        "TSTART": Date.now()
    });

    render() {
        if (this.state.streamInfo == null) return <Skeleton />

        const { classes } = this.props;
        const streamInfo = this.state.streamInfo;

        return (
            <Accordion variant="outlined">
                <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    aria-controls={`panel-${this.eventStreamName}-content`}
                    id={`panel-${this.eventStreamName}-header`}
                >
                    <Typography className={classes.heading}>{this.eventStreamName}</Typography>
                </AccordionSummary>

                <AccordionDetails className={classes.accordionDetails}>
                    <ErrorBoundary>
                        <div className={classes.pushEventBox}>
                            <ReactJson src={streamInfo} />
                            <Button variant="outlined" onClick={() => this.pushEvent(this._createMockEvent())} className={classes.pushEventButton}>Push some event</Button>
                        </div>
                    </ErrorBoundary>
                </AccordionDetails>
            </Accordion>
        )
    }
}
export default withStyles(useStyles)(EventStore)
