import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import Toolbar from '@material-ui/core/Toolbar';
import List from '@material-ui/core/List';
import Divider from '@material-ui/core/Divider';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import { Network_420, EdgeNodeAlt20, Calculation20, DataStructured20, MessageQueue20, AppConnectivity20 } from '@carbon/icons-react';
import { Link, useLocation } from "react-router-dom";
import {ListSubheader} from "@material-ui/core";

const drawerWidth = 260;

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

const DrawerListItem = ({ id, text, to, icon }) => {
    const location = useLocation();
    return (
        <ListItem component={Link} to={to} button key={id} selected={to === location.pathname}>
            <ListItemIcon>{icon}</ListItemIcon>
            <ListItemText primary={text}/>
        </ListItem>
    )
};

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
                    <DrawerListItem
                        id="sys-info"
                        text="General system info"
                        to="/admin/sys-info"
                        icon={<EdgeNodeAlt20 />} />
                    <DrawerListItem
                        id="raft-groups"
                        text="Raft groups"
                        to="/admin/raft-groups"
                        icon={<Network_420 />} />
                    <DrawerListItem
                        id="meta-data"
                        text="Meta data"
                        to="/admin/meta-data"
                        icon={<AppConnectivity20 />} />
                </List>
                <Divider/>
                <List
                    subheader={
                        <ListSubheader component="div">
                            Replication
                        </ListSubheader>
                    }>
                    <DrawerListItem
                        id="replicated-counter"
                        text="Counter"
                        to="/admin/replicated-counter"
                        icon={<Calculation20 />} />
                    {/*<DrawerListItem*/}
                    {/*    id="replicated-key-value-store"*/}
                    {/*    text="Key-value store"*/}
                    {/*    to="/admin/replicated-kv-store"*/}
                    {/*    icon={<DataStructured20 />} />*/}
                    <DrawerListItem
                        id="replicated-event-store"
                        text="ChronicleDB Event Store"
                        to="/admin/replicated-event-store"
                        icon={<MessageQueue20 />} />
                </List>
                <List
                    subheader={
                        <ListSubheader component="div">
                            Embedded
                        </ListSubheader>
                    }>
                    <DrawerListItem
                        id="replicated-event-store"
                        text="ChronicleDB Event Store"
                        to="/admin/embedded-event-store"
                        icon={<MessageQueue20 />} />
                </List>
            </div>
        </Drawer>
    );
}
