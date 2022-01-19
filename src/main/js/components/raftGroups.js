import React, { Component } from 'react';
import ApiClient from "../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {List, Typography} from "@material-ui/core";
import Accordion from '@material-ui/core/Accordion';
import AccordionDetails from '@material-ui/core/AccordionDetails';
import AccordionSummary from '@material-ui/core/AccordionSummary';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import {withStyles} from '@material-ui/core/styles';
import NodeList from "./nodeList";
import InfoListItem from "./common/list/infoListItem";
import LinkListItem from "./common/list/linkListItem";

const useStyles = theme => ({
    // used in nodeList.js
    muiTypography: {
        subtitle2: {
            fontWeight: 600,
        }
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

const formatStateMachineClass = stateMachineClass => {
    const stateMachineClassPackages = stateMachineClass.split('.');
    return stateMachineClassPackages[stateMachineClassPackages.length - 1];
}

const RaftGroupAccordion = ({ raftGroup, classes }) => {

    const { name, groupId, uuid, leaderId, currentLeaderTerm, storageHealthy, nodes, stateMachineClass } = raftGroup;

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
                    {/*<InfoListItem classes={classes} label="Leader ID" content={leaderId} />*/}
                    {/*<InfoListItem classes={classes} label="Current leader term" content={currentLeaderTerm} />*/}
                    <InfoListItem classes={classes} label="Storage health" content={storageHealthy ? 'Healthy' : 'Problematic'} />
                    <LinkListItem classes={classes} label="Details" content="" to={`/admin/raft-groups/${groupId}`} />
                </List>
                <NodeList nodeInfos={nodes} title="Group nodes" />
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
