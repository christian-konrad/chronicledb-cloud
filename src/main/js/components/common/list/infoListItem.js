import {ListItem, ListItemIcon, ListItemText} from "@material-ui/core";
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

const InfoListItem = ({ label, content, title, classes }) =>
    <ListItem className={classes.listItem}>
        <ListItemText className={classes.listItemText}
                      primary={<span className={classes.itemLabel}>{label}</span>}
                      secondary={content}
                      title={title} />
    </ListItem>;

export default withStyles(useStyles)(InfoListItem)
