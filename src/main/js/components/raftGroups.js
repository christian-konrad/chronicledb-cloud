import React, { Component } from 'react';
import ApiClient from "../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {List, ListItem, ListItemText, Typography} from "@material-ui/core";
import Accordion from '@material-ui/core/Accordion';
import AccordionDetails from '@material-ui/core/AccordionDetails';
import AccordionSummary from '@material-ui/core/AccordionSummary';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import {withStyles} from '@material-ui/core/styles';
import NodeList from "./nodeList";

const useStyles = theme => ({
    listItem: {
        borderBottom: '1px solid #f1f1f1',
    },
    listItemText: {
        display: 'flex',
    },
    muiTypography: {
        subtitle2: {
            fontWeight: 600,
        }
    },
    itemLabel: {
        minWidth: 240,
        display: 'inline-block',
    },
    subGroupItemLabel: {
        minWidth: 240,
        display: 'inline-block',
        fontSize: 18
    },
    accordionDetails: {
        display: 'block',
    },
});

const InfoListItem = ({ label, content, classes }) =>
    <ListItem className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{label}</span>}
                      secondary={content} />
    </ListItem>;

const RaftGroupAccordion = ({ raftGroup, classes }) => {

    const { name, uuid, leaderId, currentLeaderTerm, selfRole, roleSince, storageHealthy, nodes } = raftGroup;

    return <Accordion variant="outlined">
        <AccordionSummary
            expandIcon={<ExpandMoreIcon />}
            aria-controls={`panel-${name}-content`}
            id={`panel-${name}-header`}
        >
            <Typography className={classes.heading}>{name}</Typography>
        </AccordionSummary>
        <AccordionDetails className={classes.accordionDetails}>
            <div>
                <List>
                    <InfoListItem classes={classes} label="UUID" content={uuid} />
                    <InfoListItem classes={classes} label="Leader ID" content={leaderId} />
                    <InfoListItem classes={classes} label="Current leader" content={currentLeaderTerm} />
                    <InfoListItem classes={classes} label="Current role of this node" content={selfRole} />
                    <InfoListItem classes={classes} label="Has role since" content={`${roleSince}ms`} />
                    <InfoListItem classes={classes} label="Storage health" content={storageHealthy ? 'Healthy' : 'Problematic'} />
                </List>
                <NodeList nodeInfos={nodes} classes={classes} title="Group nodes" />
            </div>
        </AccordionDetails>
    </Accordion>
};

class RaftGroups extends Component {

    constructor(props) {
        super(props);
        this.state = { systemInfo: null };
    }

    componentDidMount() {
        ApiClient.fetchSystemInfo()
            .then(systemInfo => this.setState({ systemInfo }));
    }

    render() {
        if (!this.state.systemInfo) return <Skeleton />

        const { classes } = this.props;
        const { raftGroups } = this.state.systemInfo;

        return (
            <>
                {raftGroups.map(raftGroup => <RaftGroupAccordion raftGroup={raftGroup} classes={classes} /> )}
            </>
        )
    }
}
export default withStyles(useStyles)(RaftGroups)
