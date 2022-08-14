import {ListItem, ListItemIcon, ListItemText} from "@material-ui/core";
import {Link} from "react-router-dom";
import NavigateNextIcon from "@material-ui/icons/NavigateNext";
import React from "react";
import {withStyles} from "@material-ui/core/styles";

const useStyles = theme => ({
    listItem: {
        borderBottom: '1px solid #f1f1f1',
    },
    listItemText: {
        display: 'flex',
    },
    itemLabel: {
        minWidth: 240,
        display: 'inline-block'
    },
});

const LinkListItem = ({ label, content, title, classes, to }) =>
    <ListItem component={Link} to={to} button className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{label}</span>}
                      secondary={content}
                      title={title} />
        <ListItemIcon>
            <NavigateNextIcon />
        </ListItemIcon>
    </ListItem>;

export default withStyles(useStyles)(LinkListItem)
