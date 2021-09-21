import React, { Component } from 'react';
import {List, ListItem, ListItemIcon, ListItemText, Typography} from "@material-ui/core";
import NavigateNextIcon from '@material-ui/icons/NavigateNext';
import {Link} from "react-router-dom";

const NodeListItem = ({ nodeInfo, classes }) => {
    const {id:nodeId, host:nodeHost, metadataPort} = nodeInfo;

    return <ListItem component={Link} to={`/admin/sys-info/nodes/${nodeId}`} button className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{nodeId}</span>}
                      secondary={`${nodeHost}:${metadataPort}`}/>
        <ListItemIcon>
            <NavigateNextIcon />
        </ListItemIcon>
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
