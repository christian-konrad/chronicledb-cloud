import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import Toolbar from '@material-ui/core/Toolbar';
import List from '@material-ui/core/List';
import Divider from '@material-ui/core/Divider';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import InboxIcon from '@material-ui/icons/MoveToInbox';
import MailIcon from '@material-ui/icons/Mail';
import { Network_420, EdgeNodeAlt20, Calculation20 } from '@carbon/icons-react';
import {Link} from "react-router-dom";

const drawerWidth = 240;

const useStyles = makeStyles((theme) => ({
    drawer: {
        width: drawerWidth,
        flexShrink: 0,
    },
    drawerPaper: {
        width: drawerWidth,
    },
    drawerContainer: {
        overflow: 'auto',
        marginTop: 8,
    },
    listItemText: {
        fontWeight: 500,
    }
}));

export default function MainDrawer() {
    const classes = useStyles();

    return (
        <Drawer
            className={classes.drawer}
            variant="permanent"
            classes={{
                paper: classes.drawerPaper,
            }}
        >
            <Toolbar/>
            <div className={classes.drawerContainer}>
                <List>
                    <ListItem component={Link} to="/admin/sys-info" button key="sys-info">
                        <ListItemIcon><EdgeNodeAlt20 /></ListItemIcon>
                        <ListItemText primary="General system info"/>
                    </ListItem>
                    <ListItem component={Link} to="/admin/raft-groups" button key="raft-groups">
                        <ListItemIcon><Network_420 /></ListItemIcon>
                        <ListItemText primary="Raft groups"/>
                    </ListItem>
                </List>
                <Divider/>
                <List>
                    <ListItem button key="replicated-counter">
                        <ListItemIcon><Calculation20 /></ListItemIcon>
                        <ListItemText primary="Replicated counter"/>
                    </ListItem>
                </List>
            </div>
        </Drawer>
    );
}
