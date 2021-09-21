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
        display: 'inline-block'
    },
    subGroupItemLabel: {
        minWidth: 240,
        display: 'inline-block',
        fontSize: 18
    },
    accordionDetails: {
        display: 'block'
    },
    serverTitle: {
        marginBottom: 14,
        fontSize: "16px"
    },
    stateMachineTitle: {
        margin: "14px 0",
        fontSize: "14px",
        fontWeight: "bold"
    }
});

const InfoListItem = ({ label, content, title, classes }) =>
    <ListItem className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{label}</span>}
                      secondary={content}
                      title={title} />
    </ListItem>;

const formatStateMachineClass = stateMachineClass => {
    const stateMachineClassPackages = stateMachineClass.split('.');
    return stateMachineClassPackages[stateMachineClassPackages.length - 1];
}

const RaftGroupAccordion = ({ raftGroup, classes }) => {

    const { name, groupId, uuid, leaderId, currentLeaderTerm, selfRole, roleSince, storageHealthy, nodes, stateMachineClass } = raftGroup;

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
                    <InfoListItem classes={classes} label="Group ID" content={groupId} />
                    <InfoListItem classes={classes} label="State machine class" title={stateMachineClass} content={formatStateMachineClass(stateMachineClass)} />
                    <InfoListItem classes={classes} label="Leader ID" content={leaderId} />
                    <InfoListItem classes={classes} label="Current leader term" content={currentLeaderTerm} />
                    {/*<InfoListItem classes={classes} label="Current role of this node" content={selfRole} /> TODO displays wrong info*/}
                    <InfoListItem classes={classes} label="Has role since" content={`${roleSince}ms`} />
                    <InfoListItem classes={classes} label="Storage health" content={storageHealthy ? 'Healthy' : 'Problematic'} />
                </List>
                <NodeList nodeInfos={nodes} classes={classes} title="Group nodes" />
            </div>
        </AccordionDetails>
    </Accordion>
};

const RaftGroupStateMachineBlock = ({ stateMachineClass, server, raftGroups, classes }) =>
    <>
        <Typography variant="h4" className={classes.stateMachineTitle}>{formatStateMachineClass(stateMachineClass)}</Typography>
        {raftGroups.filter(raftGroup => raftGroup.serverName === server && raftGroup.stateMachineClass === stateMachineClass).map(raftGroup => <RaftGroupAccordion raftGroup={raftGroup} classes={classes} /> )}
    </>;

const RaftGroupServerBlock = ({ server, raftGroups, classes }) =>
    <>
        <Typography variant="h3" className={classes.serverTitle}>Server: <b>{server}</b></Typography>
        {[...new Set(raftGroups.filter(raftGroup => raftGroup.serverName === server).map(raftGroup => raftGroup.stateMachineClass))]
            .map(stateMachineClass => <RaftGroupStateMachineBlock server={server} stateMachineClass={stateMachineClass} raftGroups={raftGroups} classes={classes} />)}
    </>;

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

        const servers = [...new Set(raftGroups.map(raftGroup => raftGroup.serverName))];

        return (
            <>
                {servers.map(server => <RaftGroupServerBlock server={server} raftGroups={raftGroups} classes={classes} />)}
            </>
        )
    }
}
export default withStyles(useStyles)(RaftGroups)
