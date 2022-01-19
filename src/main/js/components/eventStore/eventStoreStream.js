import React, { Component } from 'react';
import ApiClient from "../../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {
    Accordion,
    AccordionDetails,
    AccordionSummary,
    Button, FormControl, InputLabel, MenuItem, Select,
    Typography
} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import ErrorBoundary from "../error/errorBoundary";
import ReactJson from 'react-json-view';
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import {Add20} from "@carbon/icons-react";
import Plot from 'react-plotly.js';

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
    plotContainer: {
        height: 480,
        display: 'flex',
        justifyContent: 'center'
    },
    pushEventButton: {
        marginTop: 24,
        background: 'white',
        marginLeft: 'auto'
    },
    buttonContainer: {
        display: 'flex'
    },
    streamDisplay: {
        lineHeight: '34px',
        fontWeight: 600
    },
    streamName: {
        lineHeight: '34px',
        marginRight: 'auto'
    },
    streamSummary: {
        marginBottom: 14,
        fontSize: "16px"
    },
    aggregateSelections: {
        display: 'flex',
        gap: 16
    }
});

const ThroughputPlot = ({ bins }) => {
    if (!bins || !bins.length) return <Skeleton/>;

    return <Plot
        data={[
            {type: 'bar', x: bins.map(bin => (new Date(bin.lower / 1000000)).toISOString()), y: bins.map(bin => bin.aggregatedCount)},
        ]}
        layout={{title: 'Throughput'}}
        // config={{responsive: true}}
    />;
};

const AGGREGATE_FUNCTIONS = {
    COUNT: "COUNT",
    SUM: "SUM",
    MIN: "MIN",
    MAX: "MAX"
};

const AGGREGATE_PERIOD = {
    mins: (count) => ({
        unit: 'mins',
        displayUnit: 'min',
        count
    }),
    hours: (count) => ({
        unit: 'hours',
        displayUnit: 'h',
        count
    }),
};

const SELECTABLE_AGGREGATE_PERIODS = [
    AGGREGATE_PERIOD.mins(1),
    AGGREGATE_PERIOD.mins(5),
    AGGREGATE_PERIOD.mins(30),
    AGGREGATE_PERIOD.hours(1),
    AGGREGATE_PERIOD.hours(6),
    AGGREGATE_PERIOD.hours(12),
    AGGREGATE_PERIOD.hours(24),
];

const _formatPeriod = (period) => `${period.count}:${period.unit}`;

const _millisAsNanos = (millis) => millis * 1000000;

const AggregateSelections = ({ classes, onAggregateTypeChange, onAggregateAttributeChange, onAggregatePeriodChange, schema, selectedAggregateType, selectedAggregateAttribute, selectedAggregatePeriod }) => {
    return <div className={classes.aggregateSelections}>
        <FormControl variant="outlined" size="small" sx={{ m: 1, minWidth: 120 }}>
            <InputLabel id="aggregate-graph-aggregate-select-label">Aggregate</InputLabel>
            <Select
                labelId="aggregate-graph-aggregate-select-label"
                id="aggregate-graph-aggregate-select"
                value={selectedAggregateType}
                label="Aggregate"
                onChange={onAggregateTypeChange}
            >
                {Object.keys(AGGREGATE_FUNCTIONS).map(aggregate => <MenuItem value={aggregate}>{aggregate}</MenuItem>)}
            </Select>
        </FormControl>
        <FormControl variant="outlined" size="small" sx={{ m: 1, minWidth: 120 }}>
            <InputLabel id="aggregate-graph-attribute-select-label">Attribute</InputLabel>
            <Select
                labelId="aggregate-graph-attribute-select-label"
                id="aggregate-graph-attribute-select"
                value={selectedAggregateAttribute}
                label="Attribute"
                onChange={onAggregateAttributeChange}
            >
                <MenuItem value="ALL">ALL</MenuItem>
                {schema.filter(attribute => ['INTEGER', 'FLOAT', 'DOUBLE'].includes(attribute.type)).map(attribute => <MenuItem value={attribute.name}>{attribute.name}</MenuItem>)}
            </Select>
        </FormControl>
        <FormControl variant="outlined" size="small" sx={{ m: 1, minWidth: 120 }}>
            <InputLabel id="aggregate-graph-period-select-label">Period</InputLabel>
            <Select
                labelId="aggregate-graph-period-select-label"
                id="aggregate-graph-period-select"
                value={_formatPeriod(selectedAggregatePeriod)}
                label="Aggregate"
                onChange={onAggregatePeriodChange}
            >
                {SELECTABLE_AGGREGATE_PERIODS.map(period => <MenuItem value={_formatPeriod(period)}>{period.count} {period.displayUnit}</MenuItem>)}
            </Select>
        </FormControl>
        {/* TODO select bins */}
    </div>;
}

class EventStoreStream extends Component {
    eventStreamName;

    constructor(props) {
        super(props);
        this.eventStreamName = props.eventStreamName;
        this.state = {
            streamInfo: null,
            bins: [],
            aggregateGraphAggregateType: AGGREGATE_FUNCTIONS.COUNT,
            aggregateGraphAttributeName: 'ALL',
            aggregateGraphPeriod: AGGREGATE_PERIOD.mins(1)
        };
        this.updateStreamInfo = this.updateStreamInfo.bind(this);
        this.getThroughputData = this.getThroughputData.bind(this);
        this.pushEvent = this.pushEvent.bind(this);
        this.handleAggregateTypeChange = this.handleAggregateTypeChange.bind(this);
        this.handleAggregateAttributeChange = this.handleAggregateAttributeChange.bind(this);
        this.handleAggregatePeriodChange = this.handleAggregatePeriodChange.bind(this);
    }

    updateStreamInfo() {
        ApiClient.fetchEventStreamInfo(this.eventStreamName)
            .then(streamInfo => this.setState({ streamInfo }))
            .catch(error => this.setState({ streamInfo: null }));
    }

    componentDidMount() {
        this.updateStreamInfo();
        // do not wait
        this.getThroughputData();
        this.interval = setInterval(async () => {
            this.updateStreamInfo();
            await this.getThroughputData();
        }, 2000); // TODO make configurable
    }

    componentWillUnmount() {
        clearInterval(this.interval);
    }

    handleAggregateTypeChange(event) {
        this.setState({ aggregateGraphAggregateType: event.target.value });
    }

    handleAggregateAttributeChange(event) {
        this.setState({ aggregateGraphAttributeName: event.target.value });
    }

    handleAggregatePeriodChange(event) {
        const [periodCount, periodUnit] = event.target.value.split(':');
        const period = AGGREGATE_PERIOD[periodUnit](periodCount);
        this.setState({ aggregateGraphPeriod: period });
    }

    // TODO remove mock event
    pushEvent(event) {
        ApiClient.pushEvent(this.eventStreamName, event)
            .then(() => this.updateStreamInfo());
    }

    // TODO improve: parametrize with scale so one can check hourly, daily, or weekly rates
    async getAggregateBins(maxBins = 100, period = AGGREGATE_PERIOD.hours(24), attributeName = 'ALL', aggregateType = AGGREGATE_FUNCTIONS.COUNT) {
        if (!this.state.streamInfo) return;

        const { timeInterval } = this.state.streamInfo;

        if (!timeInterval) return;

        if (attributeName === 'ALL' && aggregateType !== 'COUNT') return;
        // TODO validate other operations depending on attribute type

        console.log(`Getting ${aggregateType} aggregates for ${attributeName}`);

        // TODO mark timeInterval.upper in the graph

        const nowNanos = _millisAsNanos(Date.now());

        let periodMillis;
        switch (period.unit) {
            case "mins":
                periodMillis = period.count * 60000;
                break;
            case "hours":
            default:
                periodMillis = period.count * 3600000;
        }
        const periodLower = Math.max(nowNanos - _millisAsNanos(periodMillis), timeInterval.lower);

        const timeBetween = nowNanos - periodLower;

        const binDuration = Math.floor(timeBetween / maxBins);

        const bins = await Promise.all([...Array(maxBins).keys()].map(async (offset) => {
            const lower = periodLower + offset * binDuration;
            const upper = Math.min(periodLower + (offset + 1) * binDuration, nowNanos);
            let aggregatedCount = await ApiClient.getAggregate({
                streamName: this.eventStreamName,
                aggregateType,
                attributeName,
                range: { lower, upper }
            });
            aggregatedCount = parseInt(aggregatedCount) || 0;
            return { lower, upper, aggregatedCount };
        }));

        this.setState({ bins });
    }

    async getThroughputData(maxBins = 100) {
        return this.getAggregateBins(maxBins, this.state.aggregateGraphPeriod, this.state.aggregateGraphAttributeName, this.state.aggregateGraphAggregateType);
    }

    _createMockEvent = () => ({
        "someBool": !!Math.round(Math.random()),
        "someInt": Math.round(Math.random() * 10),
        "someString": "foo",
        "TSTART": _millisAsNanos(Date.now())
    });

    render() {
        if (this.state.streamInfo == null) return <Skeleton />

        const { classes } = this.props;
        const streamInfo = this.state.streamInfo;
        const { schema, eventCount, timeInterval } = streamInfo;



        const isAggregationGraphSelectionValid = this.state.aggregateGraphAttributeName !== 'ALL' || this.state.aggregateGraphAggregateType === 'COUNT';
        // TODO validate other operations depending on attribute type

        const formatDate = (millisOrNanos) => {
            // determine if millis or nanos by length of the value
            const factor = (String(millisOrNanos).length > 15) ? 1000000 : 1;
            return new Date(millisOrNanos / factor).toLocaleString();
        }

        const formattedCount = timeInterval
            ? `${eventCount} events in a time interval from ${formatDate(timeInterval.lower)} to ${formatDate(timeInterval.upper)}`
            : 'No events yet';

        return (
            <>
                <Typography variant="h3" className={classes.streamSummary}>{formattedCount}</Typography>
                <Accordion variant="outlined">
                    <AccordionSummary
                        expandIcon={<ExpandMoreIcon />}
                        aria-controls={`panel-${this.eventStreamName}-schema-content`}
                        id={`panel-${this.eventStreamName}-schema-header`}
                    >
                        <Typography className={classes.heading}>Schema</Typography>
                    </AccordionSummary>

                    <AccordionDetails className={classes.accordionDetails}>
                        <ErrorBoundary>
                            <div className={classes.pushEventBox}>
                                <ReactJson src={schema} name={false} displayDataTypes={false} />
                            </div>
                        </ErrorBoundary>
                    </AccordionDetails>
                </Accordion>

                <Accordion variant="outlined">
                    <AccordionSummary
                        expandIcon={<ExpandMoreIcon />}
                        aria-controls={`panel-${this.eventStreamName}-throughput-content`}
                        id={`panel-${this.eventStreamName}-throughput-header`}
                    >
                        <Typography className={classes.heading}>Throughput/Aggregation Graph</Typography>
                    </AccordionSummary>

                    <AccordionDetails className={classes.accordionDetails}>
                        <AggregateSelections
                            classes={classes}
                            onAggregateTypeChange={this.handleAggregateTypeChange}
                            onAggregateAttributeChange={this.handleAggregateAttributeChange}
                            onAggregatePeriodChange={this.handleAggregatePeriodChange}
                            selectedAggregateType={this.state.aggregateGraphAggregateType}
                            selectedAggregateAttribute={this.state.aggregateGraphAttributeName}
                            selectedAggregatePeriod={this.state.aggregateGraphPeriod}
                            schema={schema} />
                        <div className={classes.plotContainer}>
                            <ErrorBoundary>
                                {!timeInterval ? (
                                    <Typography variant="h4" style={{marginTop: 148}}>No events yet to show here</Typography>
                                ) : isAggregationGraphSelectionValid ? (
                                    <ThroughputPlot bins={this.state.bins} />
                                ) : (
                                    <Typography variant="h4" style={{marginTop: 148}}>Invalid aggregation selected</Typography>
                                )}
                            </ErrorBoundary>
                        </div>
                    </AccordionDetails>
                </Accordion>


                {/* TODO action bar at top right */}
                <div className={classes.buttonContainer}>
                    <Button variant="outlined" startIcon={<Add20 />} onClick={() => this.pushEvent(this._createMockEvent())} className={classes.pushEventButton}>Push some event</Button>
                </div>
                {/* TODO chart of events per time slot, use aggregates for this */}
            </>
        )
    }
}
export default withStyles(useStyles)(EventStoreStream)
