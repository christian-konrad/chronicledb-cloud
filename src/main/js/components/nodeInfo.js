import React, { Component } from 'react';
import ApiClient from "../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import {List, ListItem, ListItemText, Typography} from "@material-ui/core";
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
    }
});

const InfoListItem = ({ label, content, classes }) =>
    <ListItem className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{label}</span>}
                      secondary={content} />
    </ListItem>;

class SystemInfo extends Component {

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
        const { currentNodeId, currentNodeStoragePath, nodes } = this.state.systemInfo;

        return (
            <>
                <List>
                    <InfoListItem classes={classes} label="Current node id" content={currentNodeId} />
                    <InfoListItem classes={classes} label="Current node storage path" content={currentNodeStoragePath} />
                </List>
                <NodeList nodeInfos={nodes} classes={classes} title="Total cluster nodes" />
            </>
        )
    }
}
export default withStyles(useStyles)(SystemInfo)
