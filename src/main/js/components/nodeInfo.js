import React, { Component } from 'react';
import ApiClient from "../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {List, ListItem, ListItemText, Typography} from "@material-ui/core";
import {withStyles} from '@material-ui/core/styles';
import inflection from 'inflection';

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

const InfoListItem = ({ label, content, classes }) =>
    <ListItem className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{label}</span>}
                      secondary={content} />
    </ListItem>;

class NodeInfo extends Component {

    constructor(props) {
        super(props);
        this.state = { nodeInfo: null };
    }

    // componentDidMount() {
    //     ApiClient.fetchNodeInfo(this.props.nodeId)
    //         .then(nodeInfo => this.setState({ nodeInfo }));
    // }

    componentDidMount() {
        this.interval = setInterval(() => {
            ApiClient.fetchNodeInfo(this.props.nodeId)
                .then(nodeInfo => this.setState({ nodeInfo }));
        }, 1000);
    }

    componentWillUnmount() {
        clearInterval(this.interval);
    }

    render() {
        if (!this.state.nodeInfo) return <Skeleton />

        const { classes } = this.props;
        const { id, host, httpPort, metadataPort, replicationPort,
            storagePath, localHostAddress, localHostName,
            remoteHostAddress, osName, osVersion,
            javaVersion, jdkVersion, springVersion,
            totalDiskSpace, usableDiskSpace,
            heartbeat, health } = this.state.nodeInfo;

        const httpAddress = `${host}:${httpPort}`;
        // TODO should use https
        const httpAdminAddress = `http://${remoteHostAddress}:${httpPort}/admin`;
        const metadataAddress = `${host}:${metadataPort}`;
        const replicationAddress = `${host}:${replicationPort}`;

        const formatBytes = (bytes, decimals = 2) => {
            if (bytes === 0) return '0 Bytes';

            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

            const i = Math.floor(Math.log(bytes) / Math.log(k));

            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        }

        const totalDiskSpaceParts = totalDiskSpace.replace(/\r\n|\r|\n/g, " ").trim().split(" ");
        const usableDiskSpaceParts = usableDiskSpace.replace(/\r\n|\r|\n/g, " ").trim().split(" ");

        const diskSpaceMap = {};
        for (let i = 0; i < totalDiskSpaceParts.length; i += 3) {
            const total = parseInt(totalDiskSpaceParts[i+2].replace(/\./g, ""));
            const usable = parseInt(usableDiskSpaceParts[i+2].replace(/\./g, ""));
            diskSpaceMap[totalDiskSpaceParts[i].replace(":", "")] = {
                total,
                usable,
                totalFormatted: formatBytes(total),
                usableFormatted: formatBytes(usable),
                freePercentage: `${(usable / total * 100).toFixed(2)}%`
            }
        }

        // TODO also show on front page
        // TODO mark as red if > 30 sek, orange > 15 sec
        // TODO row "status" with connected, interrupted, disconnected
        const heartbeatBefore = (Date.now() - new Date(heartbeat).getTime()) / 1000;

        /*const status = heartbeatBefore > 30 ? 'disconnected'
            : heartbeatBefore > 10 ? 'interrupted'
                : 'connected';*/
        const status = health;
        const formattedStatus = inflection.capitalize(status);
        const formattedHeartbeatBefore = `~ ${heartbeatBefore.toFixed(0)} s`;

        // TODO may better populate this list automatically by model sent from backend
        return (
            <>
                <List>
                    <InfoListItem classes={classes} label="Node id" content={id} />
                    <InfoListItem classes={classes} label="Connection status" content={
                        <div style={{display: 'flex'}}><div className={classes.connectionStateIndicator} data-status={status.toLowerCase()} />{formattedStatus}</div>
                    } />
                    <InfoListItem classes={classes} label="Last heartbeat before" content={formattedHeartbeatBefore} />
                    <InfoListItem classes={classes} label="HTTP address" content={httpAddress} />
                    <InfoListItem classes={classes} label="Cluster management RPC address" content={metadataAddress} />
                    <InfoListItem classes={classes} label="Replication RPC address" content={replicationAddress} />
                    <InfoListItem classes={classes} label="Management web console" content={<a href={httpAdminAddress}>{httpAdminAddress}</a>} />
                    <InfoListItem classes={classes} label="Storage path" content={storagePath} />
                    <InfoListItem classes={classes} label="Local host address" content={localHostAddress} />
                    <InfoListItem classes={classes} label="Local host name" content={localHostName} />
                    <InfoListItem classes={classes} label="Remote host address" content={remoteHostAddress} />
                    <InfoListItem classes={classes} label="OS name" content={osName} />
                    <InfoListItem classes={classes} label="OS version" content={osVersion} />
                    <InfoListItem classes={classes} label="Java version" content={javaVersion} />
                    <InfoListItem classes={classes} label="JDK version" content={jdkVersion} />
                    <InfoListItem classes={classes} label="Spring version" content={springVersion} />
                    {
                        Object.keys(diskSpaceMap).map(disk =>
                            <InfoListItem classes={classes} label={`Space on ${disk}:`} content={`Total ${diskSpaceMap[disk].totalFormatted}, available ${diskSpaceMap[disk].usableFormatted} (${diskSpaceMap[disk].freePercentage})`} />
                        )
                    }
                </List>
            </>
        )
    }
}
export default withStyles(useStyles)(NodeInfo)
