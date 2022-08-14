import React, { Component } from 'react';
import {List, ListItem, ListItemIcon, ListItemText, Typography} from "@material-ui/core";
import NavigateNextIcon from '@material-ui/icons/NavigateNext';
import {Link} from "react-router-dom";
import {makeStyles} from "@material-ui/core/styles";

const useStyles = makeStyles({
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
        minWidth: 24,
        display: 'inline-block'
    },
    nodeRole: {
        minWidth: 160,
        display: 'inline-block',
        marginLeft: 18,
        color: 'gray'
    },
    nodeRoleIcon: {
        width: "20px",
        height: "20px",
        display: "inline-block",
        background: "#7d5ea5",
        verticalAlign: "bottom",
        borderRadius: "50px",
        marginRight: "18px"
    },
    nodeInfo: {
        display: 'flex',
        minWidth: 240,
        '&[data-connection-status="INTERRUPTED"]': {
            opacity: 0.6
        },
        '&[data-connection-status="DISCONNECTED"]': {
            opacity: 0.6
        },
        '&[data-connection-status="DISCONNECTED"] $nodeRoleIcon': {
            backgroundColor: 'red'
        },
        '&[data-role="LEADER"]:not([data-connection-status="DISCONNECTED"]) $nodeRoleIcon': {
            backgroundImage: "url('data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHg9IjBweCIgeT0iMHB4Igp3aWR0aD0iMjQiIGhlaWdodD0iMjQiCnZpZXdCb3g9IjAgMCAxNzIgMTcyIgpzdHlsZT0iIGZpbGw6IzAwMDAwMDsiPjxnIGZpbGw9Im5vbmUiIGZpbGwtcnVsZT0ibm9uemVybyIgc3Ryb2tlPSJub25lIiBzdHJva2Utd2lkdGg9IjEiIHN0cm9rZS1saW5lY2FwPSJidXR0IiBzdHJva2UtbGluZWpvaW49Im1pdGVyIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHN0cm9rZS1kYXNoYXJyYXk9IiIgc3Ryb2tlLWRhc2hvZmZzZXQ9IjAiIGZvbnQtZmFtaWx5PSJub25lIiBmb250LXdlaWdodD0ibm9uZSIgZm9udC1zaXplPSJub25lIiB0ZXh0LWFuY2hvcj0ibm9uZSIgc3R5bGU9Im1peC1ibGVuZC1tb2RlOiBub3JtYWwiPjxwYXRoIGQ9Ik0wLDE3MnYtMTcyaDE3MnYxNzJ6IiBmaWxsPSJub25lIj48L3BhdGg+PGcgZmlsbD0iI2ZmZmZmZiI+PHBhdGggZD0iTTg2LDEyOS42NTIxN2wzNS42MTExNywyMS40OTI4M2M1LjYxODY3LDMuMzg5ODMgMTIuNTQ4ODMsLTEuNjQ4MzMgMTEuMDU4MTcsLTguMDMzODNsLTkuNDUyODMsLTQwLjUxMzE3bDMxLjQ2ODgzLC0yNy4yNjJjNC45NTkzMywtNC4yOTI4MyAyLjMwNzY3LC0xMi40NDEzMyAtNC4yMjgzMywtMTIuOTkzMTdsLTQxLjQyMzMzLC0zLjUxMTY3bC0xNi4yMDM4MywtMzguMjM0MTdjLTIuNTU4NSwtNi4wMjcxNyAtMTEuMTAxMTcsLTYuMDI3MTcgLTEzLjY1OTY3LDBsLTE2LjIwMzgzLDM4LjIzNDE3bC00MS40MjMzMywzLjUxMTY3Yy02LjUzNiwwLjU1MTgzIC05LjE4NzY3LDguNzAwMzMgLTQuMjI4MzMsMTIuOTkzMTdsMzEuNDY4ODMsMjcuMjYybC05LjQ1MjgzLDQwLjUxMzE3Yy0xLjQ5MDY3LDYuMzg1NSA1LjQzOTUsMTEuNDIzNjcgMTEuMDU4MTcsOC4wMzM4M3oiPjwvcGF0aD48L2c+PC9nPjwvc3ZnPg==')",
            backgroundPosition: "center",
            backgroundSize: "14px",
            backgroundRepeat: "no-repeat"
        }
    },
    subGroupItemLabel: {
        minWidth: 240,
        display: 'inline-block',
        fontSize: 18
    },
});

// TODO better: regular NodeListItem and DetailedNodeListItem

const NodeListItem = ({ nodeInfo, nodeHealth, classes }) => {
    const { id:nodeId, host:nodeHost, metadataPort, division } = nodeInfo;

    const primaryComponent =
        <div className={classes.nodeInfo} data-role={division ? division.role : 'UNKNOWN'} data-connection-status={nodeHealth?.connectionState}>
            <span className={classes.nodeRoleIcon}/>
            <span className={classes.itemLabel}>{nodeId}</span>
            {!!division && <span className={classes.nodeRole}>{(!nodeHealth?.connectionState || nodeHealth?.connectionState === 'CONNECTED') ? division.role : nodeHealth.connectionState}</span>}
        </div>;
    let secondaryText = `${nodeHost}:${metadataPort}`;
    if (division) {
        secondaryText += ` - Term ${division.currentTerm}, last applied index ${division.lastAppliedIndex}`;
    }

    return <ListItem component={Link} to={`/admin/sys-info/nodes/${nodeId}`} button className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={primaryComponent}
                      secondary={secondaryText}/>
        <ListItemIcon>
            <NavigateNextIcon />
        </ListItemIcon>
    </ListItem>;
};

export default function NodeList({ nodeInfos, clusterHealth, title }) {
    const classes = useStyles();

    return <List>
        <ListItem className={classes.listItem}>
            <ListItemText className={classes.listItemText}
                          primary={<span className={classes.subGroupItemLabel}>{title}</span>}/>
        </ListItem>
        {nodeInfos.map(nodeInfo => <NodeListItem nodeInfo={nodeInfo} nodeHealth={clusterHealth && clusterHealth.nodeHealths?.find(nodeHealth => nodeHealth.id === nodeInfo.id)} classes={classes} />)}
    </List>
};
