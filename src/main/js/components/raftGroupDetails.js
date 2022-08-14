import React, { Component } from 'react';
import ApiClient from "../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {List} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import inflection from 'inflection';
import NodeList from "./nodeList";
import Link from "carbon-components-react/lib/components/UIShell/Link";
import InfoListItem from "./common/list/infoListItem";
import LinkListItem from "./common/list/linkListItem";

const useStyles = theme => ({
    muiTypography: {
        subtitle2: {
            fontWeight: 600,
        }
    },
    connectionStateIndicator: {
        width: 18,
        height: 18,
        background: '#d80f0f',
        borderRadius: '50%',
        marginTop: 1,
        marginRight: 6,
        backgroundImage: "url('data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHg9IjBweCIgeT0iMHB4Igp3aWR0aD0iNTAiIGhlaWdodD0iNTAiCnZpZXdCb3g9IjAgMCAxNzIgMTcyIgpzdHlsZT0iIGZpbGw6IzAwMDAwMDsiPjxnIGZpbGw9Im5vbmUiIGZpbGwtcnVsZT0ibm9uemVybyIgc3Ryb2tlPSJub25lIiBzdHJva2Utd2lkdGg9IjEiIHN0cm9rZS1saW5lY2FwPSJidXR0IiBzdHJva2UtbGluZWpvaW49Im1pdGVyIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHN0cm9rZS1kYXNoYXJyYXk9IiIgc3Ryb2tlLWRhc2hvZmZzZXQ9IjAiIGZvbnQtZmFtaWx5PSJub25lIiBmb250LXdlaWdodD0ibm9uZSIgZm9udC1zaXplPSJub25lIiB0ZXh0LWFuY2hvcj0ibm9uZSIgc3R5bGU9Im1peC1ibGVuZC1tb2RlOiBub3JtYWwiPjxwYXRoIGQ9Ik0wLDE3MnYtMTcyaDE3MnYxNzJ6IiBmaWxsPSJub25lIj48L3BhdGg+PGcgZmlsbD0iI2ZmZmZmZiI+PHBhdGggZD0iTTE0LjMzMzMzLDc4LjgzMzMzdjE0LjMzMzMzaDE0My4zMzMzM3YtMTQuMzMzMzN6Ij48L3BhdGg+PC9nPjwvZz48L3N2Zz4=')",
        backgroundRepeat: 'no-repeat',
        backgroundPosition: 'center',
        backgroundSize: 14,
        '&[data-status="connected"]': {
            backgroundColor: '#2fce2f',
            backgroundImage: "url('data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHg9IjBweCIgeT0iMHB4Igp3aWR0aD0iMjQiIGhlaWdodD0iMjQiCnZpZXdCb3g9IjAgMCAxNzIgMTcyIgpzdHlsZT0iIGZpbGw6IzAwMDAwMDsiPjxnIGZpbGw9Im5vbmUiIGZpbGwtcnVsZT0ibm9uemVybyIgc3Ryb2tlPSJub25lIiBzdHJva2Utd2lkdGg9IjEiIHN0cm9rZS1saW5lY2FwPSJidXR0IiBzdHJva2UtbGluZWpvaW49Im1pdGVyIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHN0cm9rZS1kYXNoYXJyYXk9IiIgc3Ryb2tlLWRhc2hvZmZzZXQ9IjAiIGZvbnQtZmFtaWx5PSJub25lIiBmb250LXdlaWdodD0ibm9uZSIgZm9udC1zaXplPSJub25lIiB0ZXh0LWFuY2hvcj0ibm9uZSIgc3R5bGU9Im1peC1ibGVuZC1tb2RlOiBub3JtYWwiPjxwYXRoIGQ9Ik0wLDE3MnYtMTcyaDE3MnYxNzJ6IiBmaWxsPSJub25lIj48L3BhdGg+PGcgZmlsbD0iI2ZmZmZmZiI+PHBhdGggZD0iTTE0NS40MzI5NCwzNy45MzI5NGwtODAuOTMyOTQsODAuOTMyOTVsLTMwLjc2NjI4LC0zMC43NjYyOGwtMTAuMTM0MTEsMTAuMTM0MTFsNDAuOTAwMzksNDAuOTAwMzlsOTEuMDY3MDYsLTkxLjA2NzA1eiI+PC9wYXRoPjwvZz48L2c+PC9zdmc+')",
        },
        '&[data-status="interrupted"]': {
            backgroundColor: '#e69711',
            backgroundImage: "none",
        }
    }
});

const formatStateMachineClass = stateMachineClass => {
    const stateMachineClassPackages = stateMachineClass.split('.');
    return stateMachineClassPackages[stateMachineClassPackages.length - 1];
}

class RaftGroupDetails extends Component {

    constructor(props) {
        super(props);
        this.state = {
            raftGroupInfo: null,
            divisions: null,
        };
    }

    componentDidMount() {
        this.interval = setInterval(() => {
            ApiClient.fetchRaftGroupInfo(this.props.raftGroupId)
                .then(raftGroupInfo => this.setState({ raftGroupInfo }));
            ApiClient.fetchRaftGroupDivisions(this.props.raftGroupId)
                .then(divisions => this.setState({ divisions }));
        }, 1000);
    }

    componentWillUnmount() {
        clearInterval(this.interval);
    }

    render() {
        if (!this.state.raftGroupInfo) return <Skeleton />

        const { classes, clusterHealth } = this.props;
        const { name, groupId, uuid, storageHealthy, serverName, stateMachineClass } = this.state.raftGroupInfo;
        let { nodes } = this.state.raftGroupInfo;
        const divisions = this.state.divisions;

        let leader = null, leaderId = "-", currentLeaderTerm = "-";
        if (divisions) {
            leader = Object.values(divisions).find(division => division.role === 'LEADER' &&
                (!clusterHealth || clusterHealth.nodeHealths?.find(nodeHealth => nodeHealth.id === division.nodeId)?.connectionState === 'CONNECTED'));
            if (leader) {
                leaderId = leader.nodeId;
                currentLeaderTerm = leader.currentTerm;
            }
            nodes = nodes.map(node => ({
                ...node,
                division: divisions[node.id]
            }));
        }

        return (
            <>
                <List>
                    <InfoListItem classes={classes} label="Name" content={name} />
                    <InfoListItem classes={classes} label="Server name" content={serverName} />
                    <InfoListItem classes={classes} label="UUID" content={uuid} />
                    <InfoListItem classes={classes} label="Group ID" content={groupId} />
                    <InfoListItem classes={classes} label="State machine class" title={stateMachineClass} content={formatStateMachineClass(stateMachineClass)} />
                    <InfoListItem classes={classes} label="Leader ID" content={leaderId} />
                    <InfoListItem classes={classes} label="Current leader term" content={currentLeaderTerm} />
                    <InfoListItem classes={classes} label="Storage health" content={storageHealthy ? 'Healthy' : 'Problematic'} />
                </List>
                <NodeList nodeInfos={nodes} clusterHealth={clusterHealth} title="Group nodes" />

                {/*<InfoListItem classes={classes} label="Connection status" content={*/}
                {/*    <div style={{display: 'flex'}}><div className={classes.connectionStateIndicator} data-status={status} />{formattedStatus}</div>*/}
                {/*} />*/}
            </>
        )
    }
}
export default withStyles(useStyles)(RaftGroupDetails)
