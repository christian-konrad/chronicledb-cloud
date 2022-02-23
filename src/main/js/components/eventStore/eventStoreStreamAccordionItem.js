import React, { Component } from 'react';
import ApiClient from "../../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {
    Accordion,
    AccordionDetails,
    AccordionSummary,
    List,
    Typography
} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import ErrorBoundary from "../error/errorBoundary";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import InfoListItem from "../common/list/infoListItem";
import LinkListItem from "../common/list/linkListItem";
import EventStoreApiClient from "../../api/eventStore/eventStoreApiClient";

const useStyles = theme => ({
    accordionDetails: {
        display: 'block'
    },
    muiTypography: {
        subtitle2: {
            fontWeight: 600,
        }
    },
});

class EventStoreStreamAccordionItem extends Component {
    eventStreamName;

    constructor(props) {
        super(props);
        this.apiClient = new EventStoreApiClient(props);
        this.eventStreamName = props.eventStreamName;
        this.state = { streamInfo: null };
        this.updateStreamInfo = this.updateStreamInfo.bind(this);
    }

    updateStreamInfo() {
        this.apiClient.fetchEventStreamInfo(this.eventStreamName)
            .then(streamInfo => this.setState({ streamInfo }))
            .catch(error => this.setState({ streamInfo: null }));
    }

    componentDidMount() {
        this.updateStreamInfo();
    }

    render() {
        if (this.state.streamInfo == null) return <Skeleton />

        const { classes, type } = this.props;
        const streamInfo = this.state.streamInfo;

        const { schema, eventCount, timeInterval } = streamInfo;

        const formattedTimeInterval = timeInterval ? `${timeInterval.lower} - ${timeInterval.upper}` : 'No events yet';

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
                        <div>
                            <List>
                                <InfoListItem classes={classes} label="Event count" content={eventCount} />
                                <InfoListItem classes={classes} label="Time interval" content={formattedTimeInterval} />
                                <InfoListItem classes={classes} label="Attributes in schema" content={schema.length} />
                                <LinkListItem classes={classes} label="Details" content="" to={`/admin/${type}-event-store/streams/${this.eventStreamName}`} />
                            </List>
                        </div>
                    </ErrorBoundary>
                </AccordionDetails>
            </Accordion>
        )
    }
}
export default withStyles(useStyles)(EventStoreStreamAccordionItem)
