import React, { Component } from 'react';
import {List, ListItem, ListItemText, Typography} from "@material-ui/core";

const NodeListItem = ({ nodeInfo, classes }) => {
    const {id:nodeId, host:nodeHost, raftPort} = nodeInfo;

    return <ListItem className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{nodeId}</span>}
                      secondary={`${nodeHost}:${raftPort}`}/>
    </ListItem>;
};

export default function NodeList({ nodeInfos, classes, title }) {
    return <List>
        <ListItem className={classes.listItem}>
            <ListItemText className={classes.listItemText}
                          primary={<span className={classes.subGroupItemLabel}>{title}</span>}/>
        </ListItem>
        {nodeInfos.map(nodeInfo => <NodeListItem nodeInfo={nodeInfo} classes={classes}/>)}
    </List>
};
